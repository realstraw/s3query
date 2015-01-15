#! /usr/bin/env python

from setuptools import setup
from s3query import __version__


setup(
    name='s3query',
    version=__version__,
    description='Read parted and encrypted S3 files like a single file object',
    long_description=open('readme.md').read(),
    author='Kexin Xie',
    author_email='ikexinxie@gmail.com',
    license='BSD',
    url='https://github.com/realstraw/s3query',
    download_url="https://github.com/realstraw/s3query/archive/v{version}"
    ".tar.gz".format(version=__version__),
    py_modules=["s3query"],
    install_requires=['boto>=2.32.1'],
    platforms=["any"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
