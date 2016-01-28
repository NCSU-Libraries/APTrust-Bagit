#!/usr/bin/python

import bagit
import os
import shutil
import sys
import yaml
import boto3
import logging
import threading
import argparse

accepted_access_levels = ['consortia', 'restricted', 'institution']
CONFIG_FILE = 'config.yml'

logging.basicConfig(filename='logs/error.log', level=logging.INFO , format='%(asctime)s - %(levelname)s:  %(message)s')

# Load config
stream = file(CONFIG_FILE, 'r')
config = yaml.load(stream)
stream.close()

#TODO: Flesh out exceptions throughout script

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

def verify_s3_upload(bag_name, env):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(config[env]['receiving_bucket'])

    if any(obj.key == bag_name for obj in bucket.objects.all()):
        return True
    else:
        raise Exception #not uploaded


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Bag a directory and send it to an APTrust S3 receiving bucket')

    parser.add_argument('directory', help='The directory to bag/ingest')
    parser.add_argument('-b', '--bag', help='Name to give the bag (default is the directory name)')
    parser.add_argument('-a', '--access', help='APTrust access level for bag (can be either: consortia, institution, or restricted)')
    parser.add_argument('-t', '--test', help='Ingest to test instance', action='store_true')
    parser.add_argument('-v', '--verbose', help='Provide more output', action='store_true')

    args = parser.parse_args()

    access = args.access or 'consortia'
    if access not in accepted_access_levels:
        logging.error('Invalid access level %s' % access)
        sys.exit(1)

    # validate directory
    if not os.path.isdir(args.directory):
        logging.error('The supplied directory does not exist')
        sys.exit(1)

    bag_dir = args.directory
    bag_name = args.bag or bag_dir.rstrip('/').split('/')[-1]

    # choose environment
    if args.test:
        env = 'test'
    else:
        env = 'production'

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

        if verify_s3_upload(bag_name, env):
            logging.info('Successfully uploaded bag to S3 - %s - from location - %s' % (bag_name, bag_dir))

    except Exception as e:
        logging.exception("There was an error:")
        sys.exit(1)

    sys.exit(0)
