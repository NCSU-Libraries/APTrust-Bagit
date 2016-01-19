#!/usr/bin/python

import os
import sys

if __name__ == '__main__':
    os.system('nohup python aptrust-bagit.py -t -v %s > aptrust.out 2> aptrust.err < /dev/null &'% sys.argv[1])
