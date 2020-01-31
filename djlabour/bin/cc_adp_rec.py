import os
import sys
import pysftp
import csv
import codecs
from datetime import datetime
import time
from time import strftime
import argparse
import logging
from logging.handlers import SMTPHandler

# prime django
import django

# django settings for shell environment
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djlabour.settings.shell")
django.setup()

# django settings for script
from django.conf import settings
from djimix.core.utils import get_connection, xsql
from djlabour.sql.cc_adp_sql import CX_VIEW_SQL, Q_CC_ADP_VERIFY, \
    INS_CC_ADP_REC
from djlabour.core.cc_adp_utilities import fn_write_adp_header, \
    fn_write_header, fn_write_row_reformatted, fn_write_error, fn_send_mail

# informix environment
os.environ['INFORMIXSERVER'] = settings.INFORMIXSERVER
os.environ['DBSERVERNAME'] = settings.DBSERVERNAME
os.environ['INFORMIXDIR'] = settings.INFORMIXDIR
os.environ['ODBCINI'] = settings.ODBCINI
os.environ['ONCONFIG'] = settings.ONCONFIG
os.environ['INFORMIXSQLHOSTS'] = settings.INFORMIXSQLHOSTS
os.environ['LD_LIBRARY_PATH'] = settings.LD_LIBRARY_PATH
os.environ['LD_RUN_PATH'] = settings.LD_RUN_PATH

# normally set as 'debug" in SETTINGS
DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""
parser = argparse.ArgumentParser(description=desc)

parser.add_argument(
    "--test",
    action='store_true',
    help="Dry run?",
    dest="test"
)
parser.add_argument(
    "-d", "--database",
    help="database name.",
    dest="database"
)

def file_download():
    if test:
        adp_csv_output = "/home/dsullivan/djlabour/djlabour/testdata/"
    else:
        adp_csv_output = settings.ADP_CSV_OUTPUT
    # sFTP fetch (GET) downloads the file from ADP file from server
    # print("Get ADP File")
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    # cnopts.hostkeys = settings.ADP_HOSTKEY
    # External connection information for ADP Application server
    XTRNL_CONNECTION = {
       'host': settings.ADP_HOST,
       'username': settings.ADP_USER,
       'password': settings.ADP_PASS,
       'cnopts': cnopts
    }
    with pysftp.Connection(**XTRNL_CONNECTION) as sftp:
        try:
            # print('Connection Established')
            sftp.chdir("adp/")
            # Remote Path is the ADP server and once logged in we fetch
            # directory listing
            remotepath = sftp.listdir()
            # Loop through remote path directory list
            # print("Remote Path = " + str(remotepath))
            for filename in remotepath:
                remotefile = filename
                # print("Remote File = " + str(remotefile))
                # set local directory for which the ADP file will be
                # downloaded to
                local_dir = ('{0}'.format(
                    adp_csv_output
                ))
                localpath = local_dir + remotefile
                # GET file from sFTP server and download it to localpath
                sftp.get(remotefile, localpath)
                #############################################################
                # Delete original file %m_%d_%y_%h_%i_%s_Applications(%c).txt
                # from sFTP (ADP) server
                #############################################################
                # sftp.remove(filename)
        except Exception as e:
            # print("Error in cc_adp_rec.py- File download, " + e.message)
            fn_write_error("Error in cc_adp_rec.py - File download, "
                           "adptocx.csv not found, " +  repr(e))
            fn_send_mail(settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
                "Error in cc_adp_rec.py - File download, "
                "adptocx.csv not found," + repr(e),
                "Error in cc_adp_rec.py - File download")

    sftp.close()


def main():

    ##########################################################################
    # ==> python cc_adp_rec.py --database=train --test
    # ==> python cc_adp_rec.py --database=cars
    ##########################################################################

    # # Defines file names and directory location
    if test:
        adp_csv_output = "/home/dsullivan/djlabour/djlabour/testdata/"
    else:
        adp_csv_output = settings.ADP_CSV_OUTPUT
        print(adp_csv_output)

    # For testing use last file
    # new_adp_file = adp_csv_output + "ADPtoCXLast.csv"
    new_adp_file = adp_csv_output + "ADPtoCX.csv"

    adp_view_file = adp_csv_output + "adptocxview.csv"
    adp_diff_file = adp_csv_output + "different.csv"
    adptocx_reformatted = adp_csv_output + "ADPtoCX_Reformatted.csv"

    # First remove yesterdays file of updates
    if os.path.isfile(adp_diff_file):
        os.remove(adp_diff_file)

    try:
        # set global variable
        global EARL
        # determines which database is being called from the command line
        if database == 'cars':
            EARL = settings.INFORMIX_ODBC
        if database == 'train':
            EARL = settings.INFORMIX_ODBC_TRAIN
        else:
            # # this will raise an error when we call get_engine()
            # below but the argument parser should have taken
            # care of this scenario and we will never arrive here.
            EARL = None
            # establish database connection
        # print(EARL)

        #################################################################
        # STEP 0--
        # Pull the file from the ADP FTP site
        # execute sftp code in production only
        #################################################################
        if not test:
            file_download()

        #################################################################
        # STEP 1--
        # Get the most recent rows from the cc_adp_rec table and write them
        # to a csv file to locate Read files and write out differences
        #################################################################
        fn_write_adp_header(adptocx_reformatted)

        #################################################################
        # NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW NEW
        # STEP 2--
        # Rewrite the ADP file formatted to match the CX constraints
        # on length and different coding and date format
        #################################################################
        with codecs.open(new_adp_file, 'r',
                         encoding='utf-8-sig') as f:
            d_reader = csv.DictReader(f, delimiter=',')
            for row in d_reader:
                fn_write_row_reformatted(adptocx_reformatted, row)
        f.close()
        # print("Created Reformatted file")

        #################################################################
        # STEP 3--
        # Instead of using the ADP last file for comparison, use instead
        # the data that is currently in cc_adp_rec so we know we are current
        #################################################################
        fn_write_adp_header(adp_csv_output + "adptocxview.csv")

        connection = get_connection(EARL)
        with connection:
            data_result = xsql(
                CX_VIEW_SQL, connection,
                key=settings.INFORMIX_DEBUG
            ).fetchall()
        ret = list(data_result)

        with open(adp_view_file, 'a') as file_out:
            csvWriter = csv.writer(file_out, delimiter=',',
                                   dialect='myDialect')
            for row in ret:
                csvWriter.writerow(row)
        file_out.close()
        # print("Created view file")

        #################################################################
        # Read in both files and compare
        # the codecs function prevents the header from ADP getting
        # into the comparison - needed because of extra characters in header
        #################################################################
        fn_write_header(adp_diff_file)
        with codecs.open(adptocx_reformatted, 'r',
                        encoding='utf-8-sig') as t1, codecs.open(adp_view_file,
                'r', encoding='utf-8-sig') as t2:

            newfile = t1.readlines()
            oldfile = t2.readlines()
            # print("Diff file created")
            # This uses sets to compare the two files
            # returns additions or changes in new but not in original
            bigb = set(newfile) - set(oldfile)

            with open(adp_diff_file, 'a') as file_out:
                for line_no, line in enumerate(bigb):
                    # x = line.split(',')
                    file_out.write(line)

            # close the files
            t1.close()
            t2.close()
            file_out.close()

        #################################################################
        # STEP 4--
        # Open differences file and start loop through records
        #################################################################

        with open(adp_diff_file, 'r') as f:
            d_reader = csv.DictReader(f, delimiter=',')

            try:
                for row in d_reader:
                    if row["job_title_code"] == "":
                        # print('No Job for = {0}, '
                        #       'Fullname = {1}'.format(row["carth_id"],
                        #                               row["payroll_name"]))
                        pass
                    else:
                        print('carthid = {0}, '
                              'Fullname = {1}'.format(row["carth_id"],
                                                        row["payroll_name"]))

                        if row["carth_id"] == "":
                            SUBJECT = 'No Carthage ID'
                            BODY = "No Carthage ID for " + row['payroll_name']
                            print("No Carthage ID for " +
                            row['payroll_name'])
                            fn_write_error("No Carthage ID for "
                                           + row['payroll_name'])
                            sendmail(settings.ADP_TO_EMAIL,
                            settings.ADP_FROM_EMAIL,
                                BODY, SUBJECT
                            )

                        elif row["file_number"] == "":
                            fn_write_error("No ADP File Number for "
                                           + row['payroll_name'])
                            SUBJECT = 'No ADP File Number'
                            BODY = "No ADP File Number for " + \
                                   row['payroll_name']
                            # sendmail(settings.ADP_TO_EMAIL,
                            # settings.ADP_FROM_EMAIL,
                            #          BODY, SUBJECT)
                        else:

                            ##################################################
                            # STEP 4a--
                            # Make sure record is not already in cc_adp_rec
                            # Limitations on filtering the ADP report
                            # allow rare cases
                            # of identical rows in report.
                            ##################################################
                            # try:

                            verifyqry = Q_CC_ADP_VERIFY(row)
                            # print(verifyqry)
                            # break

                            connection = get_connection(EARL)
                            with connection:
                                data_result = xsql(
                                    verifyqry, connection,
                                    key=settings.INFORMIX_DEBUG
                                ).fetchall()
                            ret = list(data_result)
                            print(ret)
                            # if ret is None:
                            if len(ret) == 0:
                                print("No Matching Record found - Insert")
                                ##############################################
                                # STEP 4b--
                                # Write entire row to cc_adp_rec table
                                ##############################################
                                try:
                                    INS_CC_ADP_REC(row, EARL)
                                except Exception as e:
                                    fn_write_error("Error in adptcx.py while "
                                                   "inserting into cc_adp_rec "
                                                   "Error = " + repr(e))
                                    continue

                            else:
                                pass
                                print("Found Record - do not insert duplicate")

            except Exception as e:
                # print(repr(e))
                fn_write_error("Error in cc_adp_rec.py Step 4, Error = "
                               + repr(e))
                fn_send_mail(settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
                         "Error in cc_adp_rec.py, at reading diff file.  "
                         "Error = " + repr(e),
                         "Error in cc_adp_rec.py")

        f.close()

    except Exception as e:
        # print("Error in cc_adp_rec.py, Error = " + repr(e))
        fn_write_error("Error in cc_adp_rec.py - Main: "
                       + repr(e))
        fn_send_mail(settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
                 "Error in cc_adp_rec.py, Error = " + repr(e),
                 "Error in cc_adp_rec.py")

if __name__ == "__main__":
    args = parser.parse_args()
    test = args.test
    database = args.database

if not database:
    print("mandatory option missing: database name\n")
    exit(-1)
else:
    database = database.lower()

if database != 'cars' and database != 'train' and database != 'sandbox':
    print("database must be: 'cars' or 'train' or 'sandbox'\n")
    parser.print_help()
    exit(-1)

sys.exit(main())
