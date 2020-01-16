import os
import sys
import pysftp
import datetime
from datetime import date
import time
from time import strftime
from djequis.adp.utilities import fn_write_log, fn_write_error


# python path
# sys.path.append('/usr/lib/python2.7/dist-packages/')
# sys.path.append('/usr/lib/python2.7/')
# sys.path.append('/usr/local/lib/python2.7/dist-packages/')
# sys.path.append('/data2/django_1.11/')
# sys.path.append('/data2/django_projects/')
# sys.path.append('/data2/django_third/')

# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djequis.settings")

# django settings for script
from django.conf import settings
from django.db import connections

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# sFTP fetch (GET) downloads the file from ADP file from server
def file_download():
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    # External connection information for ADP Application server
    XTRNL_CONNECTION = {
        'host': settings.ADP_HOST,
        'username': settings.ADP_USER,
        'password': settings.ADP_PASS,
        'cnopts': cnopts
    }

    ############################################################################
    # sFTP GET downloads the CSV file from ADP server and saves in local directory.
    ############################################################################
    try:
        with pysftp.Connection(**XTRNL_CONNECTION) as sftp:
            sftp.chdir("adp/")
            # Remote Path is the ADP server and once logged in we fetch directory listing
            remotepath = sftp.listdir()
            # Loop through remote path directory list
            for filename in remotepath:
                remotefile = filename
                # set local directory for which the ADP file will be downloaded to
                local_dir = ('{0}'.format(
                    settings.ADP_CSV_OUTPUT
                ))
                localpath = local_dir + remotefile
                # GET file from sFTP server and download it to localpath
                sftp.get(remotefile, localpath)
                #############################################################
                # Delete original file %m_%d_%y_%h_%i_%s_Applications(%c).txt
                # from sFTP (ADP) server
                #############################################################
                # sftp.remove(filename)
        sftp.close()

    #    file_download()

    except Exception as e:
        # print(e)
        fn_write_error("Error in adp_ftp.py - Error  = " + e.message)
        return(0)

