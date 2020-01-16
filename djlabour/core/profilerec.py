import datetime
from datetime import date
from datetime import datetime, timedelta
import time
from time import strftime

# django settings for script
from django.conf import settings

# from djequis.core.utils import sendmail
from djzbar.utils.informix import do_sql
from djzbar.utils.informix import get_engine

# Imports for additional modules and functions written as part of this project
from djequis.adp.utilities import fn_validate_field, fn_convert_date, \
    fn_calculate_age, fn_write_error, fn_write_log

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# write out the .sql file
scr = open("apdtocx_output.sql", "a")

def fn_process_profile_rec(id, ethnicity, sex, race, birth_date,
        prof_last_upd_date, EARL):
    engine = get_engine(EARL)

    try:
        ##########################################################
        #  Find out if record exists to determine update vs insert
        ##########################################################
        prof_rslt = fn_validate_field(id, "id", "id",
                                      "profile_rec", "integer", EARL)
        print("Prof Result = " + str(prof_rslt))
        # create race dictionary
        if race is None:
            # fn_write_error("Race is None")
            v_race = 'UN'
        else:
            v_race = race

        # create ethnicity dictionary
        if ethnicity is None:
            is_hispanic = 'N'
        # elif ethnicity == '':
        #     is_hispanic = 'N'
        else:
            is_hispanic = ethnicity
            # print(is_hispanic)
        if birth_date is None or birth_date.strip() == "" or len(birth_date) == 0:
            b_date = None
            print ("Empty Birthdate")
            age = None
        else:
            age = fn_calculate_age(birth_date)
            b_date = birth_date
        # print("Age = " + str(age))

        if prof_rslt is None or prof_rslt == 0:
            # Insert or update as needed
            q_insert_prof_rec = '''
              INSERT INTO profile_rec (id, sex, race, hispanic, birth_date, 
                age, prof_last_upd_date)
              VALUES (?, ?, ?, ?, ?, ?, ?) '''
            q_ins_prof_args=(id, sex, v_race, is_hispanic, b_date, age,
                             prof_last_upd_date)
            # print(q_insert_prof_rec)
            # print(q_ins_prof_args)
            engine.execute(q_insert_prof_rec, q_ins_prof_args)
            fn_write_log("Inserted into profile_rec table values " + str(id)
                         + ", " + v_race + ", " + str(is_hispanic));
            # print("Inserted into profile_rec table values " + str(id) + ","
            # + v_race + ", " + str(is_hispanic))
            scr.write(q_insert_prof_rec + '\n' + str(q_ins_prof_args) + '\n')
        else:
            q_update_prof_rec = '''
                       UPDATE profile_rec SET sex = ?,
                           hispanic = ?, race = ?,
                           birth_date = ?, age = ?,
                           prof_last_upd_date = ?
                           WHERE id = ?'''
            q_upd_prof_args = (sex, is_hispanic, v_race,
                b_date, age, prof_last_upd_date, id)
            # print(q_update_prof_rec)
            # print(q_upd_prof_args)
            engine.execute(q_update_prof_rec, q_upd_prof_args)
            fn_write_log("Updated profile_rec table values " + str(id) + ","
                         + v_race + ", " + str(is_hispanic));
            scr.write(q_update_prof_rec + '\n' + str(q_upd_prof_args) + '\n')

        return 1

    except Exception as e:
        print(e)
        fn_write_error("Error in profilerec.py for ID " + str(id)
                       + ", Error = " + e.message)
        return 0
    # finally:
    #     logging.shutdown()