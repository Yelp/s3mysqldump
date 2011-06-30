# Copyright 2009-2011 Yelp
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
"""Mercilessly taunt an Amazonian river dolphin.

This is by no means a complete mock of boto; it has only the methods I need
to make tests work. This was copied from mrjob, but is subtly different.

If you need a more extensive set of mock boto objects, we recommend adding
some sort of sandboxing feature to boto, rather than extending these somewhat
ad-hoc mock objects.
"""
from __future__ import with_statement

import boto.exception

### S3 ###

def add_mock_s3_data(mock_s3_fs, data):
    """Update mock_s3_fs (which is just a dictionary mapping bucket to
    key to contents) with a map from bucket name to key name to data."""
    for bucket_name, key_name_to_bytes in data.iteritems():
        mock_s3_fs.setdefault(bucket_name, {})
        bucket = mock_s3_fs[bucket_name]

        for key_name, bytes in key_name_to_bytes.iteritems():
            bucket[key_name] = bytes


class MockS3Connection(object):
    """Mock out boto.s3.Connection
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None,
                 host=None, debug=0, https_connection_factory=None,
                 calling_format=None, path='/', provider='aws',
                 bucket_class=None, mock_s3_fs=None):
        """Mock out a connection to S3. Most of these args are the same
        as for the real S3Connection, and are ignored.

        You can set up a mock filesystem to share with other objects
        by specifying mock_s3_fs. The mock filesystem is just a map
        from bucket name to key name to bytes.
        """
        self.mock_s3_fs = {} if mock_s3_fs is None else mock_s3_fs
        self.endpoint = host or 's3.amazonaws.com'
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def get_bucket(self, bucket_name):
        if bucket_name in self.mock_s3_fs:
            return MockBucket(connection=self, name=bucket_name)
        else:
            raise boto.exception.S3ResponseError(404, 'Not Found')

    def create_bucket(self, bucket_name, headers=None, location='',
                      policy=None):
        if bucket_name in self.mock_s3_fs:
            raise boto.exception.S3CreateError(409, 'Conflict')
        else:
            self.mock_s3_fs[bucket_name] = {}


class MockBucket:
    """Mock out boto.s3.Bucket
    """
    def __init__(self, connection=None, name=None):
        """You can optionally specify a 'data' argument, which will instantiate
        mock keys and mock data. data should be a map from key name to bytes.
        """
        self.name = name
        self.connection = connection

    def new_key(self, key_name):
        mock_s3_fs = self.connection.mock_s3_fs

        if key_name not in mock_s3_fs[self.name]:
            mock_s3_fs[self.name][key_name] = ''
        return MockKey(bucket=self, name=key_name)

    def get_key(self, key_name):
        mock_s3_fs = self.connection.mock_s3_fs

        if key_name in mock_s3_fs[self.name]:
            return MockKey(bucket=self, name=key_name)
        else:
            return None


class MockKey(object):
    """Mock out boto.s3.Key"""

    def __init__(self, bucket=None, name=None):
        """You can optionally specify a 'data' argument, which will fill
        the key with mock data.
        """
        self.bucket = bucket
        self.name = name

    def set_contents_from_file(self, f):
        mock_s3_fs = self.bucket.connection.mock_s3_fs
        f.seek(0)
        contents = f.read()
        mock_s3_fs[self.bucket.name][self.name] = contents
