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
from djequis.adp.utilities import fn_validate_field, fn_check_duplicates, \
    fn_write_log, fn_write_error

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""
# write out the .sql file
scr = open("apdtocx_output.sql", "a")
# set start_time in order to see how long script takes to execute
start_time = time.time()

################################################
# Start of processing
################################################
def fn_process_cvid(carthid, adpid, ssn, adp_assoc_id, EARL):
    engine = get_engine(EARL)

    try:
        ##############################################################
        # Inserts or updates as needed into cvid_rec
        ##############################################################

        # Validate the cx_id
        v_cx_id = fn_validate_field(carthid, "cx_id", "cx_id", "cvid_rec",
                    "integer", EARL)
        #print("CX_ID = " + str(carthid))
        #print("v_CX_ID = " + str(v_cx_id))

        # Should also check for duplicates of the adp_id and associate_id
        # What to do in that case?
        v_adp_match = fn_check_duplicates(adpid, "adp_id", "cx_id", "cvid_rec",
                                 v_cx_id, "char", EARL)
        #print("Found_ID = " + str(v_adp_match))

        # By definition, associate ID cannot be a duplicate in ADP, but
        # possibility of duplicate might exist in CX
        v_assoc_match = fn_check_duplicates(adp_assoc_id, "adp_associate_id",
                       "cx_id", "cvid_rec", v_cx_id, "char", EARL)
        #print("Found ID = " + str(v_assoc_match))

        if v_cx_id == 0 and v_assoc_match == 0 and v_adp_match == 0:
            # Insert or update as needed to ID_rec
            q_insert_cvid_rec = '''
              INSERT INTO cvid_rec (old_id, old_id_num, adp_id, ssn, cx_id, 
              cx_id_char, adp_associate_id) 
              VALUES (?,?,?,?,?,?,?)'''
            q_insert_cvid_args = (carthid, carthid, adpid, ssn, carthid, carthid, adp_assoc_id)
            # print(q_insert_cvid_rec)
            engine.execute(q_insert_cvid_rec, q_insert_cvid_args)
            scr.write(q_insert_cvid_rec + '\n' + str(q_insert_cvid_args) + '\n')
            fn_write_log("Inserted into cvid_rec table, all cx ID fields = " +
                         str(v_cx_id) + ", ADP ID = " + str(adpid) +
                         ", Associate ID + " + adp_assoc_id)

        elif str(v_cx_id) != v_assoc_match and v_assoc_match != 0:
            print('Duplicate Associate ID found')
            fn_write_log('Duplicate Associate ID found' + carthid)
        elif str(v_cx_id) != str(v_adp_match) and v_adp_match != 0:
            fn_write_log('Duplicate Associate ID found' + carthid)
            print('Duplicate ADP ID found')
        else:
            q_update_cvid_rec = '''
              UPDATE cvid_rec 
              SET old_id = ?, old_id_num = ?, adp_id = ?, ssn = ?, 
              adp_associate_id = ? 
              WHERE cx_id = ?'''
            q_update_cvid_args = (carthid, carthid, adpid, ssn, adp_assoc_id, carthid)
            # print(q_update_cvid_rec)
            fn_write_log("Updated cvid_rec table, all cx id fields = " +
                         str(v_cx_id) + ", ADP ID = " + str(adpid) +
                         ", Associate ID = " + adp_assoc_id)
            # logger.info("Update cvid_rec table");
            scr.write(q_update_cvid_rec + '\n' + str(q_update_cvid_args) + '\n')
            engine.execute(q_update_cvid_rec, q_update_cvid_args)

        return 1

    except Exception as e:
        print(e)
        fn_write_error("Error in cvidrec.py for " + carthid + " Error = "
                       + e.message)
        return 0
    # finally:
    #     logging.shutdown()
