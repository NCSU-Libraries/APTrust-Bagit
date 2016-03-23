#!/usr/bin/python

import bagit
import os
import shutil
import sys
import yaml
import boto3
import requests
import logging
import threading
import argparse
import datetime
import time
import json

accepted_access_levels = ['consortia', 'restricted', 'institution']
CONFIG_FILE = 'config.yml'


# Load config
stream = file(CONFIG_FILE, 'r')
config = yaml.load(stream)
stream.close()


logging.basicConfig(filename=config['log_file'], level=logging.INFO , format='%(asctime)s - %(levelname)s:  %(message)s')

""" Generate an aptrust-info.txt file required by APTrust """
def generate_aptrust_info(bag_path, title, access='consortia'):
    if access not in accepted_access_levels:
        raise Exception

    if not title:
        raise Exception

    with open(os.path.join(bag_path, 'aptrust-info.txt'), 'w') as aptrust_info:
        aptrust_info.write("Title: {0}\nAccess: {1}".format(title, access.capitalize()))

    return True


""" Push a bag to APTrust S3 bucket """
def push_to_aptrust(tarred_bag, bag_name, env='test', verbose=False):
    s3 = boto3.resource('s3')

    if verbose:
        s3.meta.client.upload_file(tarred_bag, config[env]['receiving_bucket'], bag_name, Callback=ProgressPercentage(tarred_bag))
    else:
        s3.meta.client.upload_file(tarred_bag, config[env]['receiving_bucket'], bag_name)

    return True


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            print "%s %s / %s (%.2f%%)" % (self._filename, self._seen_so_far, self._size, percentage)


""" Tar a bag according to APTrust specs (no compression) """
def tar_bag(bag_path, tar_path=None):
    if not tar_path:
        tar_path = os.path.normpath(bag_path) + '.tar'
    bag_dir = bag_path.rstrip('/').split('/')[-1]
    return_val = os.system('tar cf {0} --directory={1} {2}'.format(tar_path, config['bags_base_dir'], bag_dir))
    return bag_path + '.tar' if return_val == 0 else False


""" Generate bag name """
def generate_bag_name(directory_name, multipart_num=None, total_num=None):
    # convert dir name . to _
    converted_dirname = directory_name.lower().replace('.', '_')
    logging.debug('Converted directory name: %s' % converted_dirname)

    # prefix w/ institution id
    bag_name = "{0}.{1}".format(config['institution'], converted_dirname)

    # suffix w/ bag count if necessary
    if multipart_num and total_num:
        if int(multipart_num) > int(total_num):
            raise Exception

        if len(str(multipart_num)) != len(str(total_num)):
            multipart_num = multipart_num.zfill(len(str(total_num)))

        bag_name = "{0}.b{1}.of{2}".format(bag_name, multipart_num, total_num)

    return bag_name

""" Check S3 to see if the bag exists in the receiving bucket """
def verify_s3_upload(bag_name, env):
    s3 = boto3.resource('s3')

    # check a few times to make sure the data has time to appear in S3
    for _ in range(5):
        bucket = s3.Bucket(config[env]['receiving_bucket'])
        if any(obj.key == bag_name for obj in bucket.objects.all()):
            return True
        else:
            time.sleep(2)
            continue

    raise Exception #not uploaded


class DaevClient(object):
    def __init__(self, base_url):
        if requests.get(base_url).status_code != 200:
            raise Exception # can't communicate to Daev server
        self.base_url = base_url

    def create_submission_package(self, service_code, submission_datetime, assets):
        if not service_code:
            raise Exception # no service code
        if not submission_datetime:
            raise Exception # no submission datetime

        data = self._create_data_obj(service_code, submission_datetime)

        for asset in assets:
            a = self._create_asset_obj(asset)
            data['data']['relationships']['assets']['data'].append(a)

        headers = { 'Content-Type': 'application/vnd.api+json' }

        #FIXME: construct this URL properly
        r = requests.post(self.base_url + '/submission_packages', data=json.dumps(data), headers=headers)

        if r.status_code != 201:
            raise Exception # there was an issue TODO: better error

        return True

    def _create_data_obj(self, service_code, submission_datetime):
        return {
            'data': {
                'type': 'submission_packages',
                'attributes': {
                    'service_code': service_code,
                    'submission_datetime': submission_datetime,
                },
                'relationships': {
                    'assets': {
                        'data': []
                    }
                }
            }
        }

    def _create_asset_obj(self, asset):
        return {
            'attributes': {
                'filename': asset['filename'],
                'size': asset['size'],
                'location': asset['location'],
                'file_creation_datetime': asset['file_creation_datetime']
            },
            'relationships': {
                'checksums': {
                    'data': {
                        'checksum_type': 'md5',
                        'value': asset['checksum']
                    }
                }
            }
        }


def create_asset(bag_file_name, value, original_bag_dir):
    if bag_file_name.startswith('data/'):
        asset = {}
        bag_file_path = bag_file_name[5:] # cut off the data/ part
        original_file_path = os.path.join(original_bag_dir, bag_file_path)
        asset['filename'] = bag_file_name.split('/')[-1]
        asset['location'] = 'scrc-staff-prod01.lib.ncsu.edu:%s' % original_file_path #TODO: make more generic
        # get the create date
        asset['file_creation_datetime'] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(os.path.getctime(original_file_path)))
        # get size of file
        asset['size'] = os.path.getsize(original_file_path)
        # get the filename/checksum
        asset['checksum'] = value['md5']
        return asset
    else:
        return False

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Bag a directory and send it to an APTrust S3 receiving bucket')

    parser.add_argument('directory', help='The directory to bag/ingest')
    parser.add_argument('-b', '--bag', help='Name to give the bag (default is the directory name)')
    parser.add_argument('-a', '--access', help='APTrust access level for bag (can be either: consortia, institution, or restricted - default is institution)')
    parser.add_argument('-p', '--production', help='Ingest to production instance', action='store_true')
    parser.add_argument('-v', '--verbose', help='Provide more output', action='store_true')

    args = parser.parse_args()

    access = args.access or 'institution'
    if access not in accepted_access_levels:
        logging.error('Invalid access level %s' % access)
        sys.exit(1)

    # validate directory
    if not os.path.isdir(args.directory):
        logging.error('The supplied directory does not exist')
        sys.exit(1)

    bag_dir = os.path.abspath(args.directory)
    bag_name = args.bag or bag_dir.rstrip('/').split('/')[-1]

    # choose environment
    if args.production:
        env = 'production'
    else:
        env = 'test'

    # kick off bagging/ingest
    try:
        bag_dir_name = generate_bag_name(bag_name)
        new_path = os.path.join(config['bags_base_dir'], bag_dir_name)
        logging.debug('Copying directory to bag staging area')
        shutil.copytree(bag_dir, new_path)
        logging.debug('Making bag')
        the_bag = bagit.make_bag(new_path)
        generate_aptrust_info(the_bag.path, bag_name, access)
        logging.debug('Tarring bag')
        tarred_bag = tar_bag(new_path)
        logging.debug('Pushing to APTrust S3 instance (%s)' % (env))

        tar_base_name = os.path.split(tarred_bag)[1]
        push_to_aptrust(tarred_bag, tar_base_name, env, args.verbose)

        if verify_s3_upload(tar_base_name, env):
            upload_time = datetime.datetime.now().isoformat()
            assets = filter(bool, [create_asset(name, value, bag_dir) for name, value in the_bag.entries.iteritems()])
            # upload to daev
            daev_client = DaevClient(config[env]['daev_base_path'])
            daev_client.create_submission_package('apt', upload_time, assets)

            logging.info('Successfully uploaded bag to S3 - %s - from location - %s' % (bag_name, bag_dir))

            # write to audit file (tab delimited)
            with open(config['audit_file'], 'a') as audit:
              audit.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(upload_time, bag_name, tarred_bag, bag_dir, access, env))

    except Exception as e:
        logging.exception("There was an error:")
        sys.exit(1)

    sys.exit(0)
