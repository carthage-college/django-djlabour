# -*- coding: utf-8 -*-

import os, sys

# env
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djequis.settings")

from django.conf import settings


# set up command-line options

def main():
    """
    """

    try:
        print("hello world")
    except Exception as e:
        print("does not exist")
        print("Exception: {}".format(str(e)))
        sys.exit(1)


######################
# shell command line
######################

if __name__ == "__main__":

    sys.exit(main())
