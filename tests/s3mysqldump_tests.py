"""Unit tests for s3mysqldump.

These tests mock out mysqldump and s3.
"""
from __future__ import with_statement

import datetime
import logging
import os
import sys
import tempfile

import boto
import s3mysqldump

from testify import TestCase
from testify import assert_equal
from testify import assert_in
from testify import assert_raises
from testify import run
from testify import setup
from testify import teardown

from tests.mockboto import MockS3Connection
from tests.mockboto import add_mock_s3_data


class MockS3AndMysqldumpTestCase(TestCase):

    @setup
    def use_echo_as_mysqldump(self):
        """Replace mysqldump with echo, so we can what command lines
        were outputted."""
        self.monkey_patch(s3mysqldump, 'DEFAULT_MYSQLDUMP_BIN', 'echo')

    @setup
    def wrap_make_option_parser(self):
        """Wrap the option parser so that it doesn't print help or
        error messages; instead it updates self.times_help_printed
        and self.parser_errors.

        It will still exit on errors; be prepared to catch SystemExit
        """
        self.times_help_printed = 0
        self.parser_errors = []

        real_make_option_parser = s3mysqldump.make_option_parser

        def fake_print_help():
            self.times_help_printed += 1

        def fake_error(msg):
            self.parser_errors.append(msg)
            sys.exit(1)

        def wrapper():
            parser = real_make_option_parser()
            parser.print_help = fake_print_help
            parser.error = fake_error
            return parser

        self.monkey_patch(s3mysqldump, 'make_option_parser', wrapper)

    @setup
    def sandbox_s3(self):
        """Mock out the S3 filesystem. self.mock_s3_fs will be a map
        from bucket name to key name to contents.

        Also, add a single bucket named 'walrus'
        """
        self.mock_s3_fs = {}
        self.aws_access_key_id = None
        self.aws_secret_access_key = None

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs

            # keep track of credentials passed explicitly to connect_s3()
            if 'aws_access_key_id' in kwargs:
                self.aws_access_key_id = kwargs['aws_access_key_id']
            if 'aws_secret_access_key' in kwargs:
                self.aws_access_key_id = kwargs['aws_access_key_id']

            return MockS3Connection(*args, **kwargs)

        self.monkey_patch(boto, 'connect_s3', mock_boto_connect_s3)

        add_mock_s3_data(self.mock_s3_fs, {'walrus': {}})

    @setup
    def disable_s3mysqldump_logging(self):
        """Quiet logging messages from s3mysqldump."""
        self.monkey_patch(logging.getLogger('s3mysqldump'), 'disabled', True)

    @setup
    def wrap_get_current_time(self):
        """Monkey-patch datetime.datetime so that we can set current
        time with self.set_now_to() and self.set_utcnow_to()"""
        real_get_current_time = s3mysqldump.get_current_time
        self._now = None
        self._utcnow = None

        def fake_get_current_time(utc=False):
            result = self._utcnow if utc else self._now
            return result or real_get_current_time(utc=utc)

        self.monkey_patch(
            s3mysqldump, 'get_current_time', fake_get_current_time)

    def set_now_to(self, now):
        """Monkey-patch the value of now() returned by datetime.datetime.now()
        (and utcnow()). Set this to None to use current time."""
        self._now = now

    def set_utcnow_to(self, utcnow):
        """Monkey-patch the value of now() returned by datetime.datetime.now()
        (and utcnow()). Set this to None to use current time."""
        self._utcnow = utcnow

    def monkey_patch(self, obj, field, value):
        """Monkey-patch obj.field with value. This will be undone
        after each test."""
        if not hasattr(self, '_monkey_patches'):
            self._monkey_patches = []
        real_value = getattr(obj, field)
        self._monkey_patches.append((obj, field, real_value))
        setattr(obj, field, value)

    @teardown
    def un_monkey_patch(self):
        """Undo monkey patching."""
        if not hasattr(self, '_monkey_patches'):
            return
        # I suspect it's better to do this in reverse order, though
        # it may not matter
        for obj, field, real_value in reversed(self._monkey_patches):
            setattr(obj, field, real_value)

    def check_s3(self, bucket, key, args):
        """Check that mysqldump was run with the given args, and the
        contents copied to S3.
        """
        assert_in(bucket, self.mock_s3_fs)
        assert_in(key, self.mock_s3_fs[bucket])
        contents = self.mock_s3_fs[bucket][key]

        # we run "echo" in place of mysqldump, so the key's contents
        # should just be the arguments we passed to echo
        assert_equal(contents.rstrip(), args)


class TestTablesAndDatabases(MockS3AndMysqldumpTestCase):
    """Tests of specifying databases and the %D and %T options"""

    def test_basic_case(self):
        s3mysqldump.main(['foo', 's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--tables -- foo')

    def test_percent_D(self):
        s3mysqldump.main(['foo', 's3://walrus/%D.sql'])
        self.check_s3('walrus', 'foo.sql', '--tables -- foo')

    def no_percent_T_without_tables(self):
        assert_raises(SystemExit,
                      s3mysqldump.main, ['foo', 's3://walrus/%D/%T.sql'])

    def test_one_table(self):
        s3mysqldump.main(['foo', 'bar', 's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--tables -- foo bar')

    def test_percent_T_on_one_table(self):
        s3mysqldump.main(['foo', 'bar', 's3://walrus/%T.sql'])
        self.check_s3('walrus', 'bar.sql', '--tables -- foo bar')

    def test_percent_D_and_T_on_one_table(self):
        s3mysqldump.main(['foo', 'bar', 's3://walrus/%D/%T.sql'])
        self.check_s3('walrus', 'foo/bar.sql', '--tables -- foo bar')

    def test_many_tables(self):
        s3mysqldump.main(['foo', 'bar', 'baz', 'qux',
                          's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--tables -- foo bar baz qux')

    def test_percent_T_on_many_tables(self):
        s3mysqldump.main(['foo', 'bar', 'baz', 'qux',
                          's3://walrus/%T.sql'])
        self.check_s3('walrus', 'bar.sql', '--tables -- foo bar')
        self.check_s3('walrus', 'baz.sql', '--tables -- foo baz')
        self.check_s3('walrus', 'qux.sql', '--tables -- foo qux')

    def test_percent_D_and_T_on_many_tables(self):
        s3mysqldump.main(['foo', 'bar', 'baz', 'qux',
                          's3://walrus/%D/%T.sql'])
        self.check_s3('walrus', 'foo/bar.sql', '--tables -- foo bar')
        self.check_s3('walrus', 'foo/baz.sql', '--tables -- foo baz')
        self.check_s3('walrus', 'foo/qux.sql', '--tables -- foo qux')

    def test_one_database(self):
        s3mysqldump.main(['-B', 'foo', 's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--databases -- foo')

    def test_percent_D_with_one_database(self):
        s3mysqldump.main(['-B', 'foo', 's3://walrus/%D.sql'])
        self.check_s3('walrus', 'foo.sql', '--databases -- foo')

    def test_no_percent_T_with_databases_mode(self):
        assert_raises(SystemExit,
                      s3mysqldump.main, ['-B', 'foo', 's3://walrus/%D/%T.sql'])

    def test_many_databases(self):
        s3mysqldump.main(['-B', 'foo1', 'foo2', 'foo3', 's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--databases -- foo1 foo2 foo3')

    def test_percent_D_with_many_databases(self):
        s3mysqldump.main(['-B', 'foo1', 'foo2', 'foo3', 's3://walrus/%D.sql'])
        self.check_s3('walrus', 'foo1.sql', '--databases -- foo1')
        self.check_s3('walrus', 'foo2.sql', '--databases -- foo2')
        self.check_s3('walrus', 'foo3.sql', '--databases -- foo3')

    def test_all_databases(self):
        s3mysqldump.main(['-A', 's3://walrus/dbs.sql'])
        self.check_s3('walrus', 'dbs.sql', '--all-databases')

    def test_no_names_with_all_databases(self):
        assert_raises(SystemExit,
                      s3mysqldump.main, ['-A', 'foo', 's3://walrus/foo.sql'])

    def test_no_percent_T_with_all_databases(self):
        assert_raises(SystemExit,
                      s3mysqldump.main, ['-A', 's3://walrus/%T.sql'])

    def test_no_percent_D_with_all_databases(self):
        assert_raises(SystemExit,
                      s3mysqldump.main, ['-A', 's3://walrus/%D.sql'])


class TestInterpolation(MockS3AndMysqldumpTestCase):

    @setup
    def set_now(self):
        self.set_now_to(datetime.datetime(2010, 6, 6, 4, 26))
        self.set_utcnow_to(datetime.datetime(2010, 6, 6, 11, 26))

    def test_date_interpolation(self):
        s3mysqldump.main(['foo', 's3://walrus/%Y/%m/%d/foo.sql'])
        self.check_s3('walrus', '2010/06/06/foo.sql', '--tables -- foo')

    def test_time_interpolation(self):
        s3mysqldump.main(['foo', 's3://walrus/%Y/%m/%d/%H:%M/foo.sql'])
        self.check_s3('walrus', '2010/06/06/04:26/foo.sql', '--tables -- foo')

    def test_utc(self):
        s3mysqldump.main(
            ['foo', '--utc', 's3://walrus/%Y/%m/%d/%H:%M/foo.sql'])
        self.check_s3('walrus', '2010/06/06/11:26/foo.sql', '--tables -- foo')

    def test_date_and_percent_D_and_T(self):
        s3mysqldump.main(['foo', 'bar', 'baz', 'qux',
                          's3://walrus/%Y/%m/%d/%D/%T.sql'])
        self.check_s3(
            'walrus', '2010/06/06/foo/bar.sql', '--tables -- foo bar')
        self.check_s3(
            'walrus', '2010/06/06/foo/baz.sql', '--tables -- foo baz')
        self.check_s3(
            'walrus', '2010/06/06/foo/qux.sql', '--tables -- foo qux')

    def test_percent_escaping(self):
        # %D, %T aren't allowed with -A, so check that we don't
        # interpret %%D and %%T as these fields
        s3mysqldump.main(['-A', 's3://walrus/%%Y%%m%%d/%Y/%m/%d/%%D%%T.sql'])
        self.check_s3(
            'walrus', '%Y%m%d/2010/06/06/%D%T.sql', '--all-databases')


class TestBotoConfig(MockS3AndMysqldumpTestCase):

    @setup
    def make_boto_cfg(self):
        _, self.boto_cfg = tempfile.mkstemp(prefix='boto.cfg')
        with open(self.boto_cfg, 'w') as f:
            f.write('[Credentials]\n')
            f.write('aws_access_key_id = 12345678910\n')
            f.write('aws_secret_access_key = ABCDEFGHIJKLMNOPQRSTUVWXYZ\n')

    @teardown
    def rm_boto_cfg(self):
        os.unlink(self.boto_cfg)

    def test_no_boto_cfg(self):
        s3mysqldump.main(['foo', 's3://walrus/foo.sql'])
        assert_equal(self.aws_access_key_id, None)
        assert_equal(self.aws_secret_access_key, None)

    def test_with_boto_cfg(self):
        s3mysqldump.main(['-b', self.boto_cfg, 'foo', 's3://walrus/foo.sql'])
        assert_equal(self.aws_access_key_id, '12345678910')
        assert_equal(self.aws_secret_access_key, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')


if __name__ == '__main__':
    run()
