#!/usr/bin/python

import bagit
import os
import shutil
import sys
import socket
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


"""
class DaevClient

A class representing a client to DAEV-management, a system for tracking asset/package/filesystem level information
for preservation. Currently only implements creating/sending a new SubmissionPackage to an instance of DAEV-management.
"""

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

"""
class ProgressPercentage
A class that represents a progress meter for S3 submissions
"""

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
def push_to_aptrust(tarred_bag, env='test', verbose=False):
    tar_base_name = os.path.split(tarred_bag)[1]
    s3 = boto3.resource('s3')

    if verbose:
        s3.meta.client.upload_file(tarred_bag, config[env]['receiving_bucket'], tar_base_name, Callback=ProgressPercentage(tarred_bag))
    else:
        s3.meta.client.upload_file(tarred_bag, config[env]['receiving_bucket'], tar_base_name)

    return tar_base_name


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

        if len(str(multipart_num)) > 3 or len(str(total_num)) > 3:
            raise Exception # can't have more than 999 bags as part of multipart bag (according to APTrust docs)
        multipart_num = str(multipart_num).zfill(3)
        total_num = str(total_num).zfill(3)

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


""" Create an asset dict from a filename for submission to DAEV """
def create_asset(bag_file_name, value, original_bag_dir):
    if bag_file_name.startswith('data/'):
        asset = {}
        bag_file_path = bag_file_name[5:] # cut off the data/ part
        original_file_path = os.path.join(original_bag_dir, bag_file_path)
        asset['filename'] = bag_file_name.split('/')[-1]
        asset['location'] = '%s:%s' % (socket.gethostname(), original_file_path)
        # get the create date
        asset['file_creation_datetime'] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(os.path.getctime(original_file_path)))
        # get size of file
        asset['size'] = os.path.getsize(original_file_path)
        # get the filename/checksum
        asset['checksum'] = value['md5']
        return asset
    else:
        return False


""" Handles copying files to the bag staging area for bag creation.
    The 'bag_dir' arg can be a directory name string, or a list of
    filename strings.
"""
def copy_files_to_staging_area(bag_dir, apt_bag_name, original_dir=''):
    logging.debug('Copying directory to bag staging area')
    apt_bag_base_path = os.path.join(config['bags_base_dir'], apt_bag_name)
    new_path = ''
    if is_single_bag(bag_dir):
        # bag the entire directory
        # create new path for bag in staging area
        new_path = os.path.join(config['bags_base_dir'], apt_bag_name)
        # copy directory to be bagged to staging area
        shutil.copytree(bag_dir, new_path)
    else:
        # its a list of files
        for f in bag_dir:
            # construct the new path in the bag staging area
            new_path = f.replace(original_dir, '')[1:]
            new_path = os.path.dirname(new_path)
            new_path = os.path.join(apt_bag_base_path, new_path)

            # create the same subdirectory structure as original directory
            if not os.path.exists(new_path):
                os.makedirs(new_path)

            # copy the original file to the staging area
            shutil.copy2(f, new_path)

    return apt_bag_base_path


""" Convenience method for detecting whether we're processing a single or
    multipart bag
"""
def is_single_bag(bag_dir):
    return type(bag_dir) is str


""" Handles creating a bag, returns a tuple with the bag itself (of class Bag),
    and the path to the bag tarfile
"""
def create_bag(bag_name, bag_dir, access, original_dir='', bag_num='', bag_total_num=''):
    apt_bag_name = generate_bag_name(bag_name, bag_num, bag_total_num)
    apt_bag_path = copy_files_to_staging_area(bag_dir, apt_bag_name, original_dir)

    # create base bag
    logging.debug('Making bag')
    if bag_num and bag_total_num:
        bag_params = {'Bag-Count': '{0} of {1}'.format(bag_num, bag_total_num)}
        the_bag = bagit.make_bag(apt_bag_path, bag_params)
    else:
        the_bag = bagit.make_bag(apt_bag_path)
    # add the aptrust required info TODO: convert to use APTrustBag class that extends the bagit.Bag class
    generate_aptrust_info(the_bag.path, apt_bag_name, access)
    # tar the newly created bag
    logging.debug('Tarring bag')
    tarred_apt_bag = tar_bag(apt_bag_path)

    # return a reference to the bag itself, and the path of the tarred bag
    return the_bag, tarred_apt_bag


""" Handles mapping out a multipart bag - we need to know the structure of each
    bag before actually creating the bags, so we can number them.
    This is a fairly simple implementation, and might not result in the smallest
    possible number of bags, but it works.
"""
def create_multipart_bags(bag_name, files, original_bag_dir, access):
    total_size = 0
    files_to_bag = []
    bags_to_process = []
    for f, file_size in files.iteritems():
        # if adding this file would exceed the threshold
        if file_size + total_size > config['multi_threshold']:
            # create bag from current to_bag files
            bags_to_process.append(files_to_bag)
            # reset to_bag files and total_size to the current file
            files_to_bag = [f]
            total_size = file_size
        else:
            files_to_bag.append(f)
            total_size += file_size

    # if there are remaining items, bag them
    if files_to_bag:
        bags_to_process.append(files_to_bag)

    # we need to know how many bags total there are before we process them
    created_bags = []
    for idx, bag in enumerate(bags_to_process):
        new_bag = create_bag(bag_name, bag, access, original_bag_dir, idx+1, len(bags_to_process))
        created_bags.append(new_bag)

    return created_bags

""" Given a directory, return a dict where keys are the filenames within that directory
    (and all subdirectories), and values are the corresponding sizes of those files.
"""
def get_files_in_directory(directory):
    file_sizes = {}
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
      for filename in filenames:
        fpath = os.path.join(dirpath, filename)
        size = os.path.getsize(fpath)
        file_sizes[fpath] = size
        total_size += size

    return file_sizes


""" Create the command line argument parser for this script and return it. """
def create_arg_parser():
    parser = argparse.ArgumentParser(
        description='Bag a directory and send it to an APTrust S3 receiving bucket')

    parser.add_argument('directory', help='The directory to bag/ingest')
    parser.add_argument('-b', '--bag', help='Name to give the bag (default is the directory name)')
    parser.add_argument('-a', '--access', help='APTrust access level for bag (can be either: consortia, institution, or restricted - default is institution)')
    parser.add_argument('-p', '--production', help='Ingest to production instance', action='store_true')
    parser.add_argument('-v', '--verbose', help='Provide more output', action='store_true')

    return parser


""" Validate arguments received from command-line, and return them """
def evaluate_args(args):
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

    return env, access, bag_dir, bag_name


if __name__ == '__main__':

    # Parse and validate command-line args
    parser = create_arg_parser()
    args = parser.parse_args()
    env, access, bag_dir, bag_name = evaluate_args(args)

    # kick off bagging/ingest
    try:
        # get list of files, check size of directory
        files = get_files_in_directory(bag_dir)
        dir_size = reduce(lambda x,y: x + y, files.itervalues())

        if dir_size > config['multi_threshold'] and len(files) > 1:
            # create multipart bags
            created_bags = create_multipart_bags(bag_name, files, bag_dir, access)
        elif dir_size > config['multi_threshold'] and len(files) <= 1:
            raise Exception # too big
        else:
            # create single bag
            created_bags = [create_bag(bag_name, bag_dir, access)]

        for bag in created_bags:
            logging.debug('Pushing to APTrust S3 instance (%s)' % (env))
            aptrust_bag_name = push_to_aptrust(bag[1], env, args.verbose)

            if verify_s3_upload(aptrust_bag_name, env):
                upload_time = datetime.datetime.now().isoformat()
                assets = filter(bool, [create_asset(name, value, bag_dir) for name, value in bag[0].entries.iteritems()])
                # upload to daev
                #daev_client = DaevClient(config[env]['daev_base_path'])
                #daev_client.create_submission_package('apt', upload_time, assets)

                logging.info('Successfully uploaded bag to S3 - %s - from location - %s' % (bag_name, bag_dir))

                # write to audit file (tab delimited)
                with open(config['audit_file'], 'a') as audit:
                  audit.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(upload_time, bag_name, bag[1], bag_dir, access, env))

    except Exception as e:
        logging.exception("There was an error:")
        sys.exit(1)

    sys.exit(0)
