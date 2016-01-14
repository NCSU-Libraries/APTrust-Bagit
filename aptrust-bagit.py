#!/usr/bin/python

import bagit
import os
import shutil
import sys
import yaml
import boto3
import logging
import threading

accepted_access_levels = ['consortia', 'restricted', 'institution']
CONFIG_FILE = 'config.yml'

logging.basicConfig()

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
def push_to_aptrust(tarred_bag, bag_name, env='test'):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(tarred_bag, config[env]['receiving_bucket'], bag_name, Callback=ProgressPercentage(tarred_bag))

    #TODO: figure out how to catch error --- more here: https://boto3.readthedocs.org/en/latest/reference/customizations/s3.html#ref-s3transfer-usage
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
    return_val = os.system('tar cf {0} {1}'.format(tar_path, bag_path))
    return bag_path + '.tar' if return_val == 0 else False


""" Generate bag name """
def generate_bag_name(directory_name, multipart_num=None, total_num=None):
    # convert dir name . to _
    converted_dirname = directory_name.lower().replace('.', '_')
    print converted_dirname

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
    bucket = s3.bucket(config[env]['receiving_bucket'])

    if any(obj.key == bag_name for obj in bucket.objects.all()):
        return True
    else:
        raise Exception #not uploaded


if __name__ == '__main__':
    #TODO build proper argument support into script w argparse or click
    #TODO: change print statements to proper debug logging
    bag_dir = sys.argv[1]
    bag_name = sys.argv[2]

    #TODO accept test flag/production flag as arg
    env = 'test'

    rc = 0
    try:
        bag_dir_name = generate_bag_name(bag_name)
        new_path = os.path.join(config['bags_base_dir'], bag_dir_name)
        print "Copying directory to bag staging area"
        shutil.copytree(bag_dir, new_path)
        print "Making bag"
        the_bag = bagit.make_bag(new_path)
        generate_aptrust_info(the_bag.path, bag_name)
        print "Tarring bag"
        tarred_bag = tar_bag(new_path)
        print "Pushing to APTrust S3 instance (%s)" % (env)
        push_to_aptrust(tarred_bag, bag_name, env)

        if verify_s3_upload(bag_name, env):
            print "Successfully uploaded bag to S3"

    except Exception as e:
        print "There was an error:", e
        rc = 1

    sys.exit(rc)
