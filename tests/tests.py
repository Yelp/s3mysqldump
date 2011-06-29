"""Unit tests for s3mysqldump.

These tests mock out mysqldump and s3.
"""
from __future__ import absolute_import

import datetime
import logging
import sys

import boto
import s3mysqldump

from testify import TestCase
from testify import assert_equal
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

        def mock_boto_connect_s3(*args, **kwargs):
            kwargs['mock_s3_fs'] = self.mock_s3_fs
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
        time with self.set_now()"""
        real_get_current_time = s3mysqldump.get_current_time
        self._now = None

        def fake_get_current_time(utc=False):
            return self._now or real_get_current_time(utc=False)

        self.monkey_patch(
            s3mysqldump, 'get_current_time', fake_get_current_time)

    def set_now(self, now):
        """Monkey-patch the value of now() returned by datetime.datetime.now()
        (and utcnow()). Set this to None to use current time."""
        self._now = now

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
        contents copied to S3."""
        contents = self.mock_s3_fs[bucket][key]
        assert_equal(contents.rstrip(), args)


class BasicTestCase(MockS3AndMysqldumpTestCase):

    def test_basic_case(self):
        s3mysqldump.main(['foo', 's3://walrus/foo.sql'])
        self.check_s3('walrus', 'foo.sql', '--tables -- foo')


if __name__ == '__main__':
    run()
