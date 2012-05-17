s3mysqldump
===========

**s3mysqldump** is a tool to dump mysql tables to S3, so they can be consumed by Elastic MapReduce, etc.

Installation
============

From source:

python setup.py install

A Simple Example
================

The following command dumps 'user' table in 'db' database to s3 bucket s3://emr-storage/. 'my.cnf' specifies mysql parameters. 'boto.cfg' is the configure file for s3 connection which specifies things like aws credentials etc.

``s3mysqldump -v --force -m my.cnf -s -b boto.cfg db  user s3://emr-storage/user.sql``



