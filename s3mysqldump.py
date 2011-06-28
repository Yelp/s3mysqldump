# Copyright 2011 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dump mysql tables to S3, so they can be consumed by Elastic MapReduce, etc.
"""
from __future__ import with_statement

import datetime
import logging
import optparse
import pipes
import re
import shlex
import subprocess
import sys
import tempfile
import time

import boto
import boto.pyami.config


__author__ = 'David Marin <dave@yelp.com>'

__version__ = '0.1'


log = logging.getLogger('s3mysqldump')


DEFAULT_MYSQLDUMP_BIN = 'mysqldump'
DEFAULT_MYSQLDUMP_OPTS = [
    '--compact',
    '--complete-insert',
    '--default_character_set=utf8',
    '--no-create-info',
    '--quick',
    '--skip-opt',
]


S3_URI_RE = re.compile(r'^s3n?://(.*?)/(.*)$')

# match directives in a strftime format string (e.g. '%Y-%m-%d')
# for fully correct handling of percent literals (e.g. don't match %T in %%T)
STRFTIME_FIELD_RE = re.compile('%(.)')


def main(args=None):
    """Run the mysqldump utility.

    :param list args: alternate command line arguments (normally we read from ``sys.argv[:1]``)
    """
    database, tables, s3_uri_format, options = parse_args(args)

    # get current time
    if options.utc:
        now = datetime.datetime.utcnow()
    else:
        now = datetime.datetime.now()

    # set up logging
    if not options.quiet:
        log_to_stream(name='s3mysqldump', debug=options.verbose)

    s3_conn = connect_s3(boto_cfg=options.boto_cfg, host=options.s3_endpoint)

    extra_opts = parse_opts(options.mysqldump_extra_opts)

    # helper function, to call once, or once per table, below
    def mysqldump_to_s3(database, tables, s3_uri):
        if not options.force and s3_key_exists(s3_conn, s3_uri):
            log.warn('%s already exists; use --force to overwrite' % (s3_uri,))
            return

        log.info('dumping %s -> %s' % (table_desc(database, tables), s3_uri))
        with tempfile.NamedTemporaryFile(prefix='s3mysqldump-') as f:
            # dump to a temp file
            success = mysqldump_to_file(
                database, tables, f,
                mysqldump_bin=options.mysqldump_bin,
                my_cnf=options.my_cnf,
                extra_opts=extra_opts)

            # upload to S3 (if mysqldump worked!)
            if success:
                log.debug('  %s -> %s' % (f.name, s3_uri))
                start = time.time()

                s3_key = make_s3_key(s3_conn, s3_uri)
                s3_key.set_contents_from_file(f, headers=headers)

                log.debug('  Done in %.1fs' % (time.time() - start))

    if has_table_field(s3_uri_format):
        for table in tables:
            s3_uri = resolve_s3_uri_format(s3_uri_format, now, database, table)
            mysqldump_to_s3(database, [table], s3_uri)
    else:
        s3_uri = resolve_s3_uri_format(s3_uri_format, now, database)
        mysqldump_to_s3(database, tables, s3_uri)


def table_desc(database, tables=None):
    """Return a description of the given database and tables, for logging"""
    if not tables:
        return database
    elif len(tables) == 1:
        return '%s.%s' % (database, tables[0])
    else:
        return '%s.{%s}' % (database, ','.join(tables))


def has_table_field(s3_uri_format):
    """Check if s3_uri_format contains %T (which is meant to be replaced)
    with table name. But don't accidentally consume percent literals
    (e.g. ``%%T``).
    """
    return 'T' in STRFTIME_FIELD_RE.findall(s3_uri_format)


def resolve_s3_uri_format(s3_uri_format, now, database, table=None):
    """Run `:py:func`~datetime.datetime.strftime` on `s3_uri_format`,
    and also replace ``%D`` with *database* and ``%T`` with table.

    :param string s3_uri_format: s3 URI, possibly with strftime fields
    :param now: current time, as a :py:class:`~datetime.datetime`
    :param string database: database name.
    :param string table: table name.
    """
    def replacer(match):
        if match.group(1) == 'D':
            return database
        elif match.group(1) == 'T' and table is not None:
            return table
        else:
            return match.group(0)

    return now.strftime(STRFTIME_FIELD_RE.sub(replacer, s3_uri_format))


def parse_args(args=None):
    """Parse command-line arguments

    :param list args: alternate command line arguments (normally we read from ``sys.argv[:1]``)

    :return: *database*, *tables*, *s3_uri*, *options*
    """
    parser = make_option_parser()
    options, args = parser.parse_args(args)

    if len(args) == 0:
        parser.print_help()
        sys.exit()

    if len(args) < 2:
        parser.error('You must specify at least db_name and s3_uri_format')

    database = args[0]
    tables = args[1:-1]
    s3_uri_format = args[-1]

    if has_table_field(s3_uri_format) and not tables:
        parser.error('If you use %T, you must specify one or more tables')

    if not S3_URI_RE.match(s3_uri_format):
        parser.error('Invalid s3_uri_format: %r' %
                     s3_uri_format)

    return database, tables, s3_uri_format, options


def connect_s3(boto_cfg=None, **kwargs):
    """Make a connection to S3 using :py:mod:`boto` and return it.

    :param string boto_cfg: Optional path to boto.cfg file to read credentials from
    :param kwargs: Optional additional keyword args to pass to :py:func:`boto.connect_s3`. Keyword args set to ``None`` will be filtered out (so we can use boto's defaults).
    """
    if boto_cfg:
        configs = boto.pyami.config.Config(path=boto_cfg)
        kwargs['aws_access_key_id'] = configs.get(
            'Credentials', 'aws_access_key_id')
        kwargs['aws_secret_access_key'] = configs.get(
            'Credentials', 'aws_secret_access_key')
    kwargs = dict((k, v) for k, v in kwargs.iteritems() if v is not None)
    return boto.connect_s3(**kwargs)


def s3_key_exists(s3_conn, s3_uri):
    bucket_name, key_name = parse_s3_uri(s3_uri)
    bucket = s3_conn.get_bucket(bucket_name)
    return bool(bucket.get_key(key_name))


def make_s3_key(s3_conn, s3_uri):
    """Get the S3 key corresponding *s3_uri*, creating it if it doesn't exist.
    """
    bucket_name, key_name = parse_s3_uri(s3_uri)
    bucket = s3_conn.get_bucket(bucket_name)
    s3_key = bucket.get_key(key_name)
    if s3_key:
        return s3_key
    else:
        return bucket.new_key(key_name)


def make_option_parser():
    usage = '%prog [options] db_name [tbl_name ...] s3_uri_format'
    description = ('Dump one or more MySQL tables to S3.' +
                   ' s3_uri_format may be a strftime() format string, e.g.' +
                   ' s3://foo/%Y/%m/%d/, for daily (or hourly) dumps. You can '
                   ' also use %D for database name and %T for table name. '
                   ' Using %T will create one key per table.')
    option_parser = optparse.OptionParser(usage=usage, description=description)

    # trying to pick short opt names that won't get confused with
    # the mysql options
    option_parser.add_option(
        '-b', '--boto-cfg', dest='boto_cfg', default=None,
        help='Alternate path to boto.cfg file (for S3 credentials). See' +
        ' http://code.google.com/p/boto/wiki/BotoConfig for details. You'
        ' can also pass in S3 credentials by setting the environment'
        ' variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.')
    option_parser.add_option(
        '-f', '--force', dest='force', default=False, action='store_true',
        help='Overwrite existing keys on S3')
    option_parser.add_option(
        '-m', '--my-cnf', dest='my_cnf', default=None,
        help='Alternate path to my.cnf (for MySQL credentials). See' +
        ' http://dev.mysql.com/doc/refman/5.5/en/option-files.html for' +
        ' details.')
    option_parser.add_option(
        '--mysqldump-bin', dest='mysqldump_bin',
        default=DEFAULT_MYSQLDUMP_BIN,
        help='alternate path to mysqldump binary')
    option_parser.add_option(
        '-M', '--mysqldump-extra-opts', dest='mysqldump_extra_opts',
        default=[], action='append',
        help='extra args to pass to mysqldump (e.g. "-e --comment -vvv").'
        ' Use -m (see above) for passwords and other credentials.')
    option_parser.add_option(
        '-q', '--quiet', dest='quiet', default=False,
        action='store_true',
        help="Don't print to stderr")
    option_parser.add_option(
        '--s3-endpoint', dest='s3_endpoint', default=None,
        help='alternate S3 endpoint to connect to (e.g. us-west-1.elasticmapreduce.amazonaws.com).')
    option_parser.add_option(
        '--utc', dest='utc', default=False, action='store_true',
        help='Use UTC rather than local time to process s3_uri_format')
    option_parser.add_option(
        '-v', '--verbose', dest='verbose', default=False,
        action='store_true',
        help='Print more messages')

    return option_parser


def parse_s3_uri(uri):
    """Parse an S3 URI into (bucket, key)

    >>> parse_s3_uri('s3://walrus/tmp/')
    ('walrus', 'tmp/')

    If ``uri`` is not an S3 URI, raise a ValueError
    """
    match = S3_URI_RE.match(uri)
    if match:
        return match.groups()
    else:
        raise ValueError('Invalid S3 URI: %s' % uri)


def log_to_stream(name=None, stream=None, format=None, level=None, debug=False):
    """Set up logging.

    :type name: str
    :param name: name of the logger, or ``None`` for the root logger
    :type stderr: file object
    :param stderr:  stream to log to (default is ``sys.stderr``)
    :type format: str
    :param format: log message format (default is '%(message)s')
    :param level: log level to use
    :type debug: bool
    :param debug: quick way of setting the log level; if true, use ``logging.DEBUG``; otherwise use ``logging.INFO``
    """
    if level is None:
        level = logging.DEBUG if debug else logging.INFO

    if format is None:
        format = '%(message)s'

    if stream is None:
        stream = sys.stderr

    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)


def parse_opts(list_of_opts):
    """Used to parse :option:`--mysql-extra-opts`. Take a list of strings
    containing space-separated arguments, parse them, and return a list
    of arguments."""
    results = []
    for opts in list_of_opts:
        results.extend(shlex.split(opts))
    return results


def mysqldump_to_file(database, tables, file, mysqldump_bin=None, my_cnf=None, extra_opts=None):
    """Run mysqldump on a single table and dump it to a file

    :param string file: file object to dump to
    :param string database: MySQL database name
    :param tables: sequence of zero or more MySQL table names. If empty, dump all tables in the database.
    :param string mysqldump_bin: alternate path to mysqldump binary
    :param string my_cnf: alternate path to my.cnf file containing options to 
    :param extra_opts: a list of additional arguments to pass to mysqldump (e.g. hostname, port, and credentials).

    By default, we pass these arguments to mysqldump: ``--compact --complete-insert --default_character_set=utf8 --no-create-info --quick --skip-opt``

    this creates a series of ``INSERT`` statements, one per line, and one row
    per statement:

    .. code-block:: sql

        INSERT INTO `foo` (`id`, `first_name`, `last_name`) VALUES (1,'David	P','Marin');
        INSERT INTO `foo` (`id`, `first_name`, `last_name`) VALUES (4,'foo\nbar',NULL);
        INSERT INTO `foo` (`id`, `first_name`, `last_name`) VALUES (5,'Ezra\'s Awesome','Froggie');
        ...

    (you can override these options with *extra_opts*)
    """
    args = []
    args.append(mysqldump_bin or DEFAULT_MYSQLDUMP_BIN)
    # --defaults-file apparently has to go before any other options
    if my_cnf:
        args.append('--defaults-file=' + my_cnf)
    args.extend(DEFAULT_MYSQLDUMP_OPTS)
    if extra_opts:
        args.extend(extra_opts)
    args.append('--tables')
    args.append('--') # don't allow stray args to be interpreted as db name
    args.append(database)
    if tables:
        args.extend(tables)

    # do it!
    log.debug('  %s > %s' % (
        ' '.join(pipes.quote(arg) for arg in args),
        getattr(file, 'name', None) or repr(file)))

    start = time.time()

    returncode = subprocess.call(args, stdout=file)

    if returncode:
        log.debug('  Failed with returncode %d' % returncode)
    else:
        log.debug('  Done in %.1fs' % (time.time() - start))

    return not returncode


if __name__ == '__main__':
    main()
