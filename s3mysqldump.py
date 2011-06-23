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
import os
import pipes
import re
import shlex
import subprocess
import sys
import tempfile

import boto
import boto.pyami.config


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


def main(args=None):
    """Run the mysqldump utility.

    :param list args: alternate command line arguments (normally we read from ``sys.argv[:1]``)
    """
    database, tables, s3_uri, options = parse_args(args)

    # set up logging
    if not options.quiet:
        log_to_stream(name='s3mysqldump', debug=options.verbose)

    bucket_name, key_prefix = parse_s3_uri(s3_uri)
    # make sure we put tables in their own "directory"
    if key_prefix and not key_prefix.endswith('/'):
        key_prefix += '/'

    # TODO: move this into its own function
    s3_kwargs = {}
    if options.s3_endpoint:
        s3_kwargs['host'] = options.s3_endpoint
    if options.boto_cfg:
        boto_cfg = boto.pyami.config.Config(path=options.boto_cfg)
        s3_kwargs['aws_access_key_id'] = boto_cfg.get(
            'Credentials', 'aws_access_key_id')
        s3_kwargs['aws_secret_access_key'] = boto_cfg.get(
            'Credentials', 'aws_secret_access_key')
    s3_conn = boto.connect_s3(**s3_kwargs)
    bucket = s3_conn.get_bucket(bucket_name)

    for table in tables:
        key_name = key_prefix + table + '.sql'

        # check if key already exists
        s3_key = bucket.get_key(key_name)
        if s3_key and not options.force:
            log.warn('s3://%s/%s already exists; use --force to overwrite' %
                     (bucket_name, key_name))
            continue
        elif not s3_key:
            s3_key = bucket.new_key(key_name)

        log.info('dumping %s.%s -> s3://%s/%s' %
                 (database, table, bucket_name, key_name))
        with tempfile.NamedTemporaryFile(prefix=table + '.sql-') as f:
            # this does its own logging
            mysqldump_to_file(
                database, table, f,
                mysqldump_bin=options.mysqldump_bin,
                extra_opts=options.mysqldump_extra_opts)

            log.debug(' %s -> s3://%s/%s' %
                     (f.name, bucket_name, key_name))
            s3_key.set_contents_from_file(f)


def parse_args(args=None):
    """Parse command-line arguments

    :param list args: alternate command line arguments (normally we read from ``sys.argv[:1]``)

    :return: *database*, *tables*, *s3_uri*, *options*
    """
    option_parser = make_option_parser()
    options, args = option_parser.parse_args(args)

    if len(args) < 3:
        option_parser.print_help()
        sys.exit(1)

    database = args[0]
    tables = args[1:-1]
    s3_uri_format = args[-1]

    if options.utc:
        now = datetime.datetime.utcnow()
    else:
        now = datetime.datetime.now()
    s3_uri = now.strftime(s3_uri_format)
  
    return database, tables, s3_uri, options


def make_option_parser():
    usage = '%prog [options] database table1 [table2 ...] s3_uri_format'
    description = ('Dump one or more MySQL tables to S3.' +
                   ' s3_uri_format may be a strftime() format string, e.g.' +
                   ' s3://foo/%Y/%m/%d/, for daily (or hourly) dumps.')
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
        help='path to mysqldump binary (default: %d)')
    option_parser.add_option(
        '-M', '--mysqldump-extra-opts', dest='mysqldump_extra_opts',
        default=DEFAULT_MYSQLDUMP_BIN,
        help='extra args to pass to mysqldump (e.g. "-e -v"). If you want' +
        " to pass in passwords, use -m (see above).")
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


def mysqldump_to_file(database, table, file, mysqldump_bin=None, my_cnf=None, extra_opts=None):
    """Run mysqldump on a single table and dump it to a file

    :param string database: MySQL database name
    :param string table: MySQL table name
    :param string file: file object to dump to
    :param string mysqldump_bin: alternate path to mysqldump binary
    :param string my_cnf: alternate path to my.cnf file containing options to 
    :param extra_opts: a list of additional arguments to pass to mysqldump (e.g. hostname, port, and credentials). This can also be a string; if so, we'll run :py:func:`shlex.split` on it.

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
    # set up args to mysqldump
    if isinstance(extra_opts, basestring):
        extra_opts = shlex.split(extra_opts)
    
    args = []
    args.append(mysqldump_bin or DEFAULT_MYSQLDUMP_BIN)
    args.extend(DEFAULT_MYSQLDUMP_OPTS)
    if my_cnf:
        args.append('--defaults-file=' + my_cnf)
    if extra_opts:
        args.extend(extra_opts)
    args.append('--') # don't allow stray args to be interpreted as db name
    args.append(database)
    args.append(table)

    # do it!
    log.debug(' %s > %s' % (
        ' '.join(pipes.quote(arg) for arg in args),
        getattr(file, 'name', None) or repr(file)))
    subprocess.check_call(args, stdout=file)


if __name__ == '__main__':
    main()
