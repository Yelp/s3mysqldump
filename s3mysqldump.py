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

__author__ = 'David Marin <dave@yelp.com>'
__version__ = '0.1'

import datetime
import glob
import logging
import optparse
import os
import pipes
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import socket

import boto
import boto.pyami.config


log = logging.getLogger('s3mysqldump')


DEFAULT_MYSQLDUMP_BIN = 'mysqldump'

SINGLE_ROW_FORMAT_OPTS = [
    # --skip-opt causes out of memory error on 5.0.91, so do explicitly instead
    # '--skip-opt',
    '--compact',
    '--complete-insert',
    '--default_character_set=utf8',
    '--hex-blob',
    '--no-create-db',
    '--no-create-info',
    '--quick',
    '--skip-lock-tables',
    '--skip-extended-insert',
]


S3_URI_RE = re.compile(r'^s3n?://(.*?)/(.*)$')

S3_MAX_PUT_SIZE = 4 * 1024 * 1024 * 1024  # actually 5G, but 4 to be safe

# match directives in a strftime format string (e.g. '%Y-%m-%d')
# for fully correct handling of percent literals (e.g. don't match %T in %%T)
STRFTIME_FIELD_RE = re.compile('%(.)')


def main(args):
    """Run the mysqldump utility.

    :param list args: alternate command line arguments (normally we read from
                      ``sys.argv[:1]``)
    """
    databases, tables, s3_uri_format, options = parse_args(args)

    now = get_current_time(utc=options.utc)

    # set up logging
    if not options.quiet:
        log_to_stream(name='s3mysqldump', debug=options.verbose)

    s3_conn = connect_s3(boto_cfg=options.boto_cfg, host=options.s3_endpoint)

    extra_opts = parse_opts(options.mysqldump_extra_opts)

    # helper function, to call once, or once per table, below
    def mysqldump_to_s3(s3_uri, databases=None, tables=None):
        if not options.force and s3_key_exists(s3_conn, s3_uri):
            log.warn('%s already exists; use --force to overwrite' % (s3_uri,))
            return

        log.info('dumping %s -> %s' % (dump_desc(databases, tables), s3_uri))
        with tempfile.NamedTemporaryFile(prefix='s3mysqldump-') as file:
            # dump to a temp file
            success = mysqldump_to_file(
                file, databases, tables,
                mysqldump_bin=options.mysqldump_bin,
                my_cnf=options.my_cnf,
                extra_opts=extra_opts,
                single_row_format=options.single_row_format)

            # upload to S3 (if mysqldump worked!)
            if success:
                log.debug('  %s -> %s' % (file.name, s3_uri))
                start = time.time()

                s3_key = make_s3_key(s3_conn, s3_uri)
                if os.path.getsize(file.name) > S3_MAX_PUT_SIZE:
                    upload_multipart(s3_key, file.name)
                else:
                    log.debug('Upload to %r' % s3_key)
                    upload_singlepart(s3_key, file.name)
                log.debug('  Done in %.1fs' % (time.time() - start))

    # output to separate files, if specified by %T and %D
    if has_table_field(s3_uri_format):
        assert len(databases) == 1
        database = databases[0]
        for table in tables:
            s3_uri = resolve_s3_uri_format(s3_uri_format, now, database, table)
            mysqldump_to_s3(s3_uri, [database], [table])
    elif has_database_field(s3_uri_format):
        for database in databases:
            s3_uri = resolve_s3_uri_format(s3_uri_format, now, database)
            mysqldump_to_s3(s3_uri, [database], tables)
    else:
        s3_uri = resolve_s3_uri_format(s3_uri_format, now)
        mysqldump_to_s3(s3_uri, databases, tables)


def get_current_time(utc=False):
    """Get the current time. This is broken out so we can monkey-patch
    it for testing."""
    if utc:
        return datetime.datetime.utcnow()
    else:
        return datetime.datetime.now()


def dump_desc(databases=None, tables=None):
    """Return a description of the given database and tables, for logging"""
    if not databases:
        return 'all databases'
    elif not tables:
        if len(databases) == 1:
            return databases[0]
        else:
            return '{%s}' % ','.join(databases)
    elif len(tables) == 1:
        return '%s.%s' % (databases[0], tables[0])
    else:
        return '%s.{%s}' % (databases[0], ','.join(tables))


def has_database_field(s3_uri_format):
    """Check if s3_uri_format contains %D (which is meant to be replaced)
    with database name. But don't accidentally consume percent literals
    (e.g. ``%%D``).
    """
    return 'D' in STRFTIME_FIELD_RE.findall(s3_uri_format)


def has_table_field(s3_uri_format):
    """Check if s3_uri_format contains %T (which is meant to be replaced)
    with table name. But don't accidentally consume percent literals
    (e.g. ``%%T``).
    """
    return 'T' in STRFTIME_FIELD_RE.findall(s3_uri_format)


def resolve_s3_uri_format(s3_uri_format, now, database=None, table=None):
    """Run `:py:func`~datetime.datetime.strftime` on `s3_uri_format`,
    and also replace ``%D`` with *database* and ``%T`` with table.

    :param string s3_uri_format: s3 URI, possibly with strftime fields
    :param now: current time, as a :py:class:`~datetime.datetime`
    :param string database: database name.
    :param string table: table name.
    """
    def replacer(match):
        if match.group(1) == 'D' and database is not None:
            return database
        elif match.group(1) == 'T' and table is not None:
            return table
        else:
            return match.group(0)

    return now.strftime(STRFTIME_FIELD_RE.sub(replacer, s3_uri_format))


def parse_args(args):
    """Parse command-line arguments

    :param list args: alternate command line arguments (normally we read from
                      ``sys.argv[1:]``)

    :return: *database*, *tables*, *s3_uri*, *options*
    """
    parser = make_option_parser()

    if not args:
        parser.print_help()
        sys.exit()

    options, args = parser.parse_args(args)

    s3_uri_format = args[-1]
    if not S3_URI_RE.match(s3_uri_format):
        parser.error('Invalid s3_uri_format: %r' % s3_uri_format)

    if options.mode == 'tables':
        if len(args) < 2:
            parser.error('You must specify at least db_name and s3_uri_format')

        databases = args[:1]
        tables = args[1:-1]
    elif options.mode == 'databases':
        if len(args) < 2:
            parser.error('You must specify at least db_name and s3_uri_format')

        databases = args[:-1]
        tables = None
    else:
        assert options.mode == 'all_databases'
        if len(args) > 1:
            parser.error("Don't specify database names with --all-databases")
        databases = None
        tables = None

    if has_table_field(s3_uri_format) and not tables:
        parser.error('If you use %T, you must specify one or more tables')

    if has_database_field(s3_uri_format) and not databases:
        parser.error('If you use %D, you must specify one or more databases'
                     ' (use %d for day of month)')

    return databases, tables, s3_uri_format, options


def connect_s3(boto_cfg=None, **kwargs):
    """Make a connection to S3 using :py:mod:`boto` and return it.

    :param string boto_cfg: Optional path to boto.cfg file to read credentials
                            from
    :param kwargs: Optional additional keyword args to pass to
                   :py:func:`boto.connect_s3`. Keyword args set to ``None``
                   will be filtered out (so we can use boto's defaults).
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


def sleeping_callback(t):
    """Return a callback function that sleeps for t seconds"""
    return lambda _, __: time.sleep(t)

S3_ATTEMPTS = 4   # number of times to retry failed uploads
S3_THROTTLE = 60  # number of times to throttle during upload


def upload_multipart(s3_key, large_file):
    """Split up a large_file into chunks suitable for multipart upload, then
    upload each chunk."""
    split_dir = tempfile.mkdtemp(prefix='s3mysqldump-split-')
    split_prefix = "%s/part-" % split_dir

    args = ['split', "--line-bytes=%u" % S3_MAX_PUT_SIZE, '--suffix-length=5',
            '-d', large_file, split_prefix]
    log.debug(' '.join(pipes.quote(arg) for arg in args))
    subprocess.check_call(args)

    mp = s3_key.bucket.initiate_multipart_upload(s3_key.name)
    log.debug('Multipart upload to %r' % s3_key)
    for part, filename in enumerate(sorted(glob.glob(split_prefix + '*'))):
        with open(filename, 'rb') as file:
            for t in xrange(S3_ATTEMPTS):
                try:
                    mp.upload_part_from_file(
                        file,
                        part + 1,
                        cb=sleeping_callback(t),
                        num_cb=S3_THROTTLE
                    )  # counting starts at 1
                    log.debug('Part %s uploaded to %r' % (part + 1, s3_key))
                    break
                except socket.error as e:
                    log.warn('Part %s, upload attempt %s/%s: '
                             'upload_part_from_file raised %r' %
                            (part + 1, t, S3_ATTEMPTS, e))
            else:
                raise socket.error("Upload failed")

    mp.complete_upload()

    shutil.rmtree(split_dir, True)


def upload_singlepart(s3_key, filename):
    """Upload a normal sized file.  Retry with sleeping callbacks when
    throttled by S3.
    """
    for t in xrange(S3_ATTEMPTS):
        try:
            s3_key.set_contents_from_filename(
                filename,
                cb=sleeping_callback(t),
                num_cb=S3_THROTTLE
            )
            break
        except socket.error as e:
            log.warn('Upload attempt %s/%s: set_contents_from_file raised %r' %
                    (t, S3_ATTEMPTS, e))
    else:
        raise socket.error("Upload failed")


def make_option_parser():
    usage = '%prog [options] db_name [tbl_name ...] s3_uri_format'
    description = ('Dump one or more MySQL tables to S3.'
                   ' s3_uri_format may be a strftime() format string, e.g.'
                   ' s3://foo/%Y/%m/%d/, for daily (or hourly) dumps. You can'
                   ' also use %D for database name and %T for table name. '
                   ' Using %T will create one key per table.')
    option_parser = optparse.OptionParser(usage=usage, description=description)

    # trying to pick short opt names that won't get confused with
    # the mysql options
    option_parser.add_option(
        '-A', '--all-databases', dest='mode', default='tables',
        action='store_const', const='all_databases',
        help='Dump all tables from all databases.')
    option_parser.add_option(
        '-B', '--databases', dest='mode', default='tables',
        action='store_const', const='databases',
        help='Dump entire databases rather than tables')
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
        ' details. You can also specify this path in the environment'
        ' variable MY_CNF.')
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
        '--tables', dest='mode', default='tables',
        action='store_const', const='tables',
        help='Dump tables from one database (the default).')
    option_parser.add_option(
        '--s3-endpoint', dest='s3_endpoint', default=None,
        help=('alternate S3 endpoint to connect to (e.g.'
              ' us-west-1.elasticmapreduce.amazonaws.com).'))
    option_parser.add_option(
        '-s', '--single-row-format', dest='single_row_format', default=False,
        action='store_true',
        help=('Output single-row INSERT statements, and turn off locking, for'
              ' easy data processing. Equivalent to -M "%s"'
              % ' '.join(SINGLE_ROW_FORMAT_OPTS)))
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


def log_to_stream(name=None, stream=None, format=None, level=None,
                  debug=False):
    """Set up logging.

    :type name: str
    :param name: name of the logger, or ``None`` for the root logger
    :type stderr: file object
    :param stderr:  stream to log to (default is ``sys.stderr``)
    :type format: str
    :param format: log message format (default is '%(message)s')
    :param level: log level to use
    :type debug: bool
    :param debug: quick way of setting the log level; if true, use
                  ``logging.DEBUG``; otherwise use ``logging.INFO``
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


def mysqldump_to_file(file, databases=None, tables=None, mysqldump_bin=None,
                      my_cnf=None, extra_opts=None, single_row_format=False):
    """Run mysqldump on a single table and dump it to a file

    :param string file: file object to dump to
    :param databases: sequence of MySQL database names, or ``None`` for all
                      databases
    :param tables: sequences of MySQL table names, or ``None`` for all tables.
                   If you specify tables, there must be exactly one database
                   name, due to limitations of :command:`mysqldump`
    :param string mysqldump_bin: alternate path to mysqldump binary
    :param string my_cnf: alternate path to my.cnf file containing MySQL
                          credentials. If not set, this function will also try
                          to read the environment variable :envvar:`MY_CNF`.
    :param extra_opts: a list of additional arguments to pass to mysqldump
                       (e.g. hostname, port, and credentials).
    :param single_row_format: Output single-row INSERT statements, and turn off
                              locking, for easy data processing.. Passes
                              ``--compact --complete-insert
                              --default_character_set=utf8 --hex-blob
                              --no-create-db --no-create-info --quick
                              --skip-lock-tables --skip-extended-insert`` to
                              :command:`mysqldump`. Note this also turns off
                              table locking. You can override any of this with
                              *extra_opts*.

    If you dump multiple databases in single-row format, you will still get one
    ``USE`` statement per database; :command:`mysqldump` doesn't have a way to
    turn this off.
    """
    if tables and (not databases or len(databases) != 1):
        raise ValueError(
            'If you specify tables you must specify exactly one database')

    args = []
    args.append(mysqldump_bin or DEFAULT_MYSQLDUMP_BIN)

    # --defaults-file apparently has to go before any other options
    if my_cnf:
        args.append('--defaults-file=' + my_cnf)
    elif os.environ.get('MY_CNF'):
        args.append('--defaults-file=' + os.environ['MY_CNF'])

    if single_row_format:
        args.extend(SINGLE_ROW_FORMAT_OPTS)
    if extra_opts:
        args.extend(extra_opts)

    if not databases and not tables:
        args.append('--all-databases')
    elif len(databases) > 1 or tables is None:
        args.append('--databases')
        args.append('--')
        args.extend(databases)
    else:
        assert len(databases) == 1
        args.append('--tables')
        args.append('--')
        args.append(databases[0])
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
    main(sys.argv[1:])
