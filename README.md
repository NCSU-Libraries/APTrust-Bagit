# aptrust-bagit

## Description

A set of scripts to bag things according to the bagit specification,
along with creation of metadata files that APTrust require. Ingests to
APTrust S3 buckets, verifies S3 upload.

## Setup

### Configuration

Copy config.yml.example to config.yml, and fill in with your own values

- bags_base_dir: a staging area where bags will be created (must have sufficient space available)
- audit_file: the location of a file which will be used to store metadata for successfully transmitted bags
- institution: your APTrust institution code (example: ncsu)
- receiving_bucket: the address of the S3 receiving bucket - you can set both test and production versions

The Python S3 client (boto3) also expects that a file with the bucket keys reside in ~/.aws/credentials in the form:

    [default]
    aws_access_key_id = access_key_here 
    aws_secret_access_key = secret_access_key_here

### Installation

    cd path/to/script
    virtualenv venv
    source venv/bin/activate
    pip install -r requirements.txt

### Running the script

This script is meant to be run inside a virtualenv (unless you install the dependencies globally), so you must activate the virtualenv before running it.
You can either do this by activating the virtualenv the same way as in the installation (source venv/bin/activate), or call the script using the virtualenvs
version of python:

    /path/to/virtualenv/bin/python aptrust-bagit.py [args here]

Here is information about the various arguments you can provide to the script:

	usage: aptrust-bagit.py [-h] [-b BAG] [-a ACCESS] [-p] [-v] directory

	Bag a directory and send it to an APTrust S3 receiving bucket

	positional arguments:
	  directory             The directory to bag/ingest

	optional arguments:
	  -h, --help            show this help message and exit
	  -b BAG, --bag BAG     Name to give the bag (default is the directory name)
	  -a ACCESS, --access ACCESS
				APTrust access level for bag (can be either:
				consortia, institution, or restricted - default is
				institution)
	  -p, --production      Ingest to production instance
	  -v, --verbose         Provide more output


#### Run with nohup

Since larger bags may take a while to transmit, it is recommended to use nohup to run this script so you can disconnect from your SSH session (if running manually).
See send_dir_to_aptrust.py for an example of how to do this, or run that script instead of aptrust-bagit.py (it defaults to submitting to the test instance of APTrust right now)

## What information is recorded

For each bag, as part of the bagging process, the following information is kept:
  - Checksums for each file in the bag (manifest-md5.txt, tagmanifest-md5.txt)
  - Bag name, access level (aptrust-info.txt)
  - Bagging date, payload oxum, bagging agent (bag-info.txt)
  - Bagit version information (bagit.txt)

In addition to this, the following information is recorded for successfully transmitted bags in audit.txt:
  - Date/Time
  - Bag Name
  - Location/Name of tar file
  - Original directory that was bagged
  - Access level
  - Whether it was submitted to test vs. production

Errors in transmission or bagging are also recorded in logs/error.log

## TODO

- Use APTrust API to verify bag appears there, get identifiers & store
  locally
