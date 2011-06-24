try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import s3mysqldump

setup(
    author='David Marin',
    author_email='dave@yelp.com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: System :: Archiving :: Mirroring',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities',
    ],
    description='Dump MySQL tables to S3, and parse them',
    install_requires=['boto>=1.9'],
    license='Apache',
    long_description=open('README.rst').read(),
    name='s3mysqldump',
    provides=['s3mysqldump'],
    py_modules=['s3mysqldump'],
    scripts=['bin/s3mysqldump'],
    url='http://github.com/Yelp/s3mysqldump',
    version=s3mysqldump.__version__,
)
