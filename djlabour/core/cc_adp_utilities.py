import csv
import warnings
import datetime
from datetime import datetime
from datetime import date
import time
from time import strftime, strptime
import argparse
import shutil
import logging
from logging.handlers import SMTPHandler

# django settings for script
from django.conf import settings

# from djequis.core.utils import sendmail
# from djzbar.utils.informix import do_sql
# from djzbar.utils.informix import get_engine

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# write out the .sql file
# scr = open("apdtocx_output.sql", "a")
# set start_time in order to see how long script takes to execute
start_time = time.time()

# create logger
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def fn_convert_date(date):
    # print(date)
    if date != "":
        ndate = datetime.strptime(date, "%Y-%m-%d")
        retdate = datetime.strftime(ndate, "%m/%d/%Y")
    else:
        retdate = ''
    # print(str(date) + ',' + str(retdate))
    return retdate


def fn_informx_date(date):
    if date != "":
        ndate = datetime.strptime(date, "%m/%d/%Y")
        retdate = datetime.strftime(ndate, "%Y-%m-%d")
    else:
        retdate = None
    # print(retdate)
    return retdate
#########################################################
# Common function to format phone for CX
#########################################################
def fn_format_phone(phone):
    try:
        if phone is None or len(phone) == 0:
            return ""
        elif phone != "":
            ph = str(phone).replace("(","")
            ph = ph.replace(")","")
            ph = ph.replace(" ","")
            ph = ph.replace("-","")
            areacode =  ph[0:3]
            prefix = ph[3:6]
            number = ph[6:10]
                 # +phone[6:9]+phone[10:14]
            # print("Area Code = " + areacode)
            # print("Prefix = " + prefix)
            # print("Number = " + number)
            v = areacode + '-' + prefix + '-' + number
            # print(v)
            return v
        else:
            return ""
    except Exception as e:
        fn_write_error("Error in cc_adp_utilities.py - fn_format_phone.  Error = "
                       + e.message)


def WRITE_HEADER(filename):
    with open(filename, 'wb') as file_out:
        # Write header row
        csvWriter = csv.writer(file_out)
        # These are the column headers that match CX, not ADP
        csvWriter.writerow(
            ["file_number", "carth_id", "last_name", "first_name",
             "middle_name", "salutation", "payroll_name",
             "preferred_name", "birth_date", "gender",
             "marital_status", "race", "race_descr", "ethnicity",
             "ethnicity_id_meth", "personal_email",
             "primary_address1", "primary_address2",
             "primary_address3", "primary_city", "primary_state_code",
             "primary_state_descr", "primary_zip", "primary_county",
             "primary_country", "primary_country_code",
             "primary_legal_address", "home_phone", "mobile_phone",
             "work_phone", "wc_work_phone", "wc_work_email",
             "use_work_for_notification", "legal_address1",
             "legal_address2", "legal_address3", "legal_city",
             "legal_state_code", "legal_state_description",
             "legal_zip", "legal_county", "legal_country",
             "legal_country_code", "ssn", "hire_date",
             "hire_rehire_date", "rehire_date", "pos_start_date",
             "pos_effective_date", "pos_effective_end_date",
             "termination_date", "position_status",
             "status_effective_date", "status_eff_end_date",
             "adj_service_date", "archived", "position_id",
             "primary_position", "payroll_comp_code",
             "payroll_comp_name", "cip", "worker_cat_code",
             "worker_cat_descr", "job_title_code", "job_title_descr",
             "home_cost_code", "home_cost_descr", "job_class_code",
             "job_class_descr", "job_description", "job_function_code",
             "job_function_description", "room_number",
             "location_code", "location_description",
             "leave_start_date", "leave_return_date",
             "home_dept_code", "home_dept_descr", "supervisor_id",
             "supervisor_fname", "supervisor_lname",
             "business_unit_code", "business_unit_descr",
             "reports_to_name", "reports_to_pos_id",
             "reports_to_assoc_id", "employee_assoc_id",
             "management_position", "supervisor_flag", "long_title"])
        file_out.close()


def WRITE_ADP_HEADER(filename):
    # print(filename)
    csv.register_dialect('myDialect',
                         quoting=csv.QUOTE_ALL,
                         skipinitialspace=True)
    with open(filename, 'wb') as file_out:
        # Write header row to match the ADP file
        csvWriter = csv.writer(file_out, dialect='myDialect')
        csvWriter.writerow(
            ["File Number", "Carthage ID #", "Last Name", "First Name",
             "Middle Name", "Salutation", "Payroll Name", "Preferred Name",
             "Birth Date", "Gender", "Marital Status Code", "Race Code",
             "Race Description", "Ethnicity", "Ethnicity/Race ID Method",
             "Personal Contact: Personal Email",
             "Primary Address: Address Line 1",
             "Primary Address: Address Line 2",
             "Primary Address: Address Line 3", "Primary Address: City",
             "Primary Address: State / Territory Code",
             "Primary Address: State / Territory Description",
             "Primary Address: Zip / Postal Code",
             "Primary Address: County", "Primary Address: Country",
             "Primary Address: Country Code",
             "Primary Address: Use as Legal / Preferred Address",
             "Personal Contact: Home Phone",
             "Personal Contact: Personal Mobile",
             "Work Phone", "Work Contact: Work Phone",
             "Work Contact: Work Email",
             "Work Contact: Use Work Email for Notification",
             "Legal / Preferred Address: Address Line 1",
             "Legal / Preferred Address: Address Line 2",
             "Legal / Preferred Address: Address Line 3",
             "Legal / Preferred Address: City",
             "Legal / Preferred Address: State / Territory Code",
             "Legal / Preferred Address: State / Territory Description",
             "Legal / Preferred Address: Zip / Postal Code",
             "Legal / Preferred Address: County",
             "Legal / Preferred Address: Country",
             "Legal / Preferred Address: Country Code",
             "Tax ID (SSN)", "Hire Date", "Hire Date/Rehire Date",
             "Rehire Date", "Position Start Date", "Position Effective Date",
             "Position Effective End Date", "Termination Date",
             "Position Status", "Status Effective Date",
             "Status Effective End Date", "Adjusted Service Date",
             "Archived Employee", "Position ID", "Primary Position",
             "Payroll Company Code", "Payroll Company Name", "CIP Code",
             "Worker Category Code", "Worker Category Description",
             "Job Title Code", "Job Title Description",
             "Home Cost Number Code", "Home Cost Number Description",
             "Job Class Code", "Job Class Description", "Job Description",
             "Job Function Code", "Job Function Description", "Room Number",
             "Location Code", "Location Description",
             "Leave of Absence Start Date", "Leave of Absence Return Date",
             "Home Department Code", "Home Department Description",
             "Supervisor ID", "Supervisor First Name",
             "Supervisor Last Name", "Business Unit Code",
             "Business Unit Description", "Reports To Name",
             "Reports To Position ID", "Reports To Associate ID",
             "Associate ID", "This is a Management position",
             "Supervisor Position", "Directory Job Title"])
    file_out.close()


def WRITE_ROW_REFORMATTED(filename, row):
    try:
        # print(str(row[0]))
        # print('carthid = {0}, Fullname = {1}'.format(row["Carthage ID"],
        #                                              row[
        #                                                  "payroll_name"]))
        # print("Use as legal {0}".format(row[
        # "primary_legal_address"]))
        ethnic_code = {
            'Not Hispanic or Latino': 'N',
            'Hispanic or Latino': 'Y'
        }

        is_hispanic = ethnic_code.get(row["Ethnicity"])
        if is_hispanic is None:
            is_hispanic = ""
        # else:
        # print("Is Hispanic = " + str(is_hispanic))

        racecode = {
            '1': 'WH',
            '2': 'BL',
            '4': 'AS',
            '5': 'AM',
            '6': 'AP',
            '9': 'MU'
        }

        race = racecode.get(row["Race Code"])
        if race is None:
            race = ""
        # print("Race = " + str(race))
        # print(row["File Number"])
        csv.register_dialect('myDialect',
                             quoting=csv.QUOTE_ALL,
                             skipinitialspace=True)
        with open(filename, 'a') as file_out:
            # Write header row to match the ADP file
            csvWriter = csv.writer(file_out, dialect='myDialect')
            csvWriter.writerow([
                row["File Number"],
                row["Carthage ID #"],
                row["Last Name"],
                row["First Name"],
                row["Middle Name"],
                row["Salutation"],
                row["Payroll Name"],
                row["Preferred Name"],
                fn_informx_date(row["Birth Date"]),
                (row["Gender"][:1]),
                row["Marital Status Code"],
                race,
                (row["Race Description"][:24]),
                is_hispanic,
                row["Ethnicity/Race ID Method"],
                (row["Personal Contact: Personal Email"][:64]),
                (row["Primary Address: Address Line 1"][:64]),
                (row["Primary Address: Address Line 2"][:64]),
                (row["Primary Address: Address Line 3"][:64]),
                row["Primary Address: City"],
                (row["Primary Address: State / Territory Code"][:2]),
                row["Primary Address: State / Territory Description"],
                row["Primary Address: Zip / Postal Code"],
                row["Primary Address: County"],
                (row["Primary Address: Country"][:25]),
                (row["Primary Address: Country Code"][:3]),
                (row["Primary Address: Use as Legal / Preferred Address"][:1]),
                fn_format_phone(row["Personal Contact: Home Phone"]),
                fn_format_phone(row["Personal Contact: Personal Mobile"]),
                fn_format_phone(row["Work Phone"]),
                fn_format_phone(row["Work Contact: Work Phone"]),
                row["Work Contact: Work Email"],
                (row["Work Contact: Use Work Email for Notification"][:1]),
                (row["Legal / Preferred Address: Address Line 1"][:64]),
                (row["Legal / Preferred Address: Address Line 2"][:64]),
                (row["Legal / Preferred Address: Address Line 3"][:64]),
                row["Legal / Preferred Address: City"],
                (row["Legal / Preferred Address: State / Territory Code"][:2]),
                row["Legal / Preferred Address: State / Territory Description"],
                row["Legal / Preferred Address: Zip / Postal Code"],
                row["Legal / Preferred Address: County"],
                (row["Legal / Preferred Address: Country"][:25]),
                (row["Legal / Preferred Address: Country Code"][:3]),
                row["Tax ID (SSN)"],
                fn_informx_date(row["Hire Date"]),
                fn_informx_date(row["Hire Date/Rehire Date"]),
                fn_informx_date(row["Rehire Date"]),
                fn_informx_date(row["Position Start Date"]),
                fn_informx_date(row["Position Effective Date"]),
                fn_informx_date(row["Position Effective End Date"]),
                fn_informx_date(row["Termination Date"]),
                row["Position Status"],
                fn_informx_date(row["Status Effective Date"]),
                fn_informx_date(row["Status Effective End Date"]),
                fn_informx_date(row["Adjusted Service Date"]),
                row["Archived Employee"], row["Position ID"],
                row["Primary Position"],
                row["Payroll Company Code"],
                row["Payroll Company Name"], row["CIP Code"],
                (row["Worker Category Code"][:4]),
                (row["Worker Category Description"][:64]),
                (row["Job Title Code"][:3]),
                (row["Job Title Description"][:125]),
                row["Home Cost Number Code"],
                row["Home Cost Number Description"],
                (row["Job Class Code"][:2]),
                row["Job Class Description"],
                row["Job Description"],
                row["Job Function Code"],
                row["Job Function Description"],
                (row["Room Number"][:4]),
                (row["Location Code"][:4]),
                (row["Location Description"][:24]),
                fn_informx_date(row["Leave of Absence Start Date"]),
                fn_informx_date(row["Leave of Absence Return Date"]),
                row["Home Department Code"],
                row["Home Department Description"],
                row["Supervisor ID"],
                row["Supervisor First Name"],
                row["Supervisor Last Name"],
                row["Business Unit Code"].zfill(3),
                row["Business Unit Description"],
                row["Reports To Name"],
                row["Reports To Position ID"],
                row["Reports To Associate ID"],
                row["Associate ID"],
                row["This is a Management position"],
                (row["Supervisor Position"][:4]),
                row["Directory Job Title"]
            ])

    except Exception as e:
        print("Error in cc_adp_utilities " + e.message)
    file_out.close()

#########################################################
# Common functions to handle logger messages and errors
#########################################################

def fn_write_error(msg):
    # create error file handler and set level to error
    handler = logging.FileHandler(
        '{0}cc_apd_rec_error.log'.format(settings.LOG_FILEPATH))
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.error(msg)
    handler.close()
    logger.removeHandler(handler)
    fn_clear_logger()
    return("Error logged")

def fn_clear_logger():
    logging.shutdown()
    return("Clear Logger")


# def fn_write_log(msg):
#     # create console handler and set level to info
#     # handler = logging.FileHandler(
#     #     '{0}apdtocx.log'.format(settings.LOG_FILEPATH))
#     # handler.setLevel(logging.INFO)
#     # formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(
#     # message)s',
#     #                               datefmt='%m/%d/%Y %I:%M:%S %p')
#     # handler.setFormatter(formatter)
#     # logger.addHandler(handler)
#     # logger.info(msg)
#     # handler.close()
#     # logger.removeHandler(handler)
#     # info_logger = logging.getLogger('info_logger')
#     # info_logger.info(msg)
#     # fn_clear_logger()
#     return("Message logged")

#
# # def sample_function(secret_parameter):
# #     logger = logging.getLogger(__name__)  # __name__=projectA.moduleB
# #     logger.debug("Going to perform magic with '%s'",  secret_parameter)
# #
# #     try:
# #         result = print(secret_parameter)
# #     except IndexError:
# #         logger.exception("OMG it happened again, someone please tell Laszlo")
# #     except:
# #         logger.info("Unexpected exception", exc_info=True)
# #         raise
# #     else:
# #         logger.info("Magic with '%s' resulted in '%s'", secret_parameter, result, stack_info=True)