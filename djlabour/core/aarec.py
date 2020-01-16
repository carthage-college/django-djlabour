import datetime
from datetime import date
from datetime import datetime, timedelta
import time

# django settings for script
from django.conf import settings

from djzbar.utils.informix import do_sql
from djzbar.utils.informix import get_engine, get_session

# Imports for additional modules and functions written as part of this project
from djequis.adp.utilities import fn_convert_date, fn_write_log, \
    fn_write_error, fn_format_phone

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# write out the .sql file
scr = open("apdtocx_output.sql", "a")
# set start_time in order to see how long script takes to execute
start_time = time.time()


######################################################
#  START HERE
######################################################
######################################################
# Dates are an issue to resolve
# Need to see if there is a record of the same type (PERM) and if so
# add an end date
# ID, AA and Start date should serve as a key of sorts, and if there is
# another record with the same start date, it will throw Duplicate key error.

# So in addition to seeing if the same address is in the database, also look
# for existing record with same aa type and find its start and stop dates.
# Add End Date to previous record because we would be
# creating a new alternate address

# Scenario 1 -  No aa_rec record exists:   Insert only
# Scenario 2 -  aa_rec is a close match of new data - update
# Scenario 3 -  aa_rec data exists but does not match,
#    end date old record and insert new
######################################################

def fn_archive_address(id, fullname, addr1, addr2, addr3, cty, st, zp, ctry,
            phone, EARL):
    try:
        # print(addr1, addr2, addr3)
        #################################
        #  See if there is already an Archive record
        #################################
        q_check_aa_adr = '''
          SELECT id, aa, aa_no, beg_date, line1, line2, line3, city, st, zip, phone, 
          ctry 
          FROM aa_rec 
          WHERE id = {0}
          AND aa in ('PERM','PREV','SCND')
          AND end_date is null
          '''.format(id)
        sql_id_address = do_sql(q_check_aa_adr, key=DEBUG, earl=EARL)
        addr_result = sql_id_address.fetchone()

        # print(q_check_aa_adr)
        # print("AA_rec Addr Result = " + str(addr_result))
        if addr_result is None or len(str(addr_result[2])) == 0:
            # print("No archive address")
            found_aa_num = 0
        else:
            found_aa_num = addr_result[2]
            fn_write_log("Existing archive address record found in aa_rec for "
                         + fullname + ", ID = " + str(id))
        # print(found_aa_num)

        #################################
        #  Find the max start date of all PREV entries with a null end date
        #################################
        q_check_aa_date = '''
          SELECT MAX(beg_date), ID, aa, line1, end_date
           AS date_end
           FROM aa_rec 
           Where id = {0}
           AND aa = 'PREV'
           AND end_date is null
           GROUP BY id, aa, end_date, line1
          '''.format(id)
        # print(q_check_aa_date)
        sql_date = do_sql(q_check_aa_date, key=DEBUG, earl=EARL)
        date_result = sql_date.fetchone()
        # print("AA Max date = " + str(date_result))

        #################################
        # Define date variables
        #################################
        if found_aa_num == 0 or date_result is None: #No aa rec found
            max_date = datetime.now().strftime("%m/%d/%Y")
        # Make sure dates don't overlap
        else:
            max_date = date.strftime(date_result[0],"%m/%d/%Y")
        # print("Max date = " + str(max_date))

        # Scenario 1
        # This means that the ID_Rec address will change
        # but nothing exists in aa_rec, so we will only insert as 'PREV'
        if found_aa_num == 0: # No address in aa rec?
              print("No existing record - Insert only")
              # print(datetime.now().strftime("%m/%d/%Y"))
              fn_insert_aa(id, fullname, 'PREV', addr1, addr2, addr3,
                           cty, st, zp, ctry,
                           datetime.now().strftime("%m/%d/%Y"),
                           fn_format_phone(phone),EARL)

        # Scenario 2
        # if record found in aa_rec, then we will need more options
        # Find out if the record is an exact match with another address
        # Question is, what is enough of a match to update rather than insert new?
        elif addr_result[4] == addr1 \
             and addr_result[9] == zp:
            # and addr_result[7] == cty \
            # and addr_result[8] == st \
            # and addr_result[10] == ctry:
            # or addr_result[5] == addr2 \
            # or addr_result[6] == addr3 \

            print("An Address exists and matches new data - Update new")
            #################################
            # Match found then we are UPDATING only....
            #################################
            fn_update_aa(id, aa, aanum, fllname, add1, add2, add3, cty, st,
                             zp, ctry, begdate, fn_format_phone(phone), EARL)

        # to avoid overlapping dates
        # Scenario 3 - AA Rec exists but does not match new address.
        # End old, insert new
        else:
            if max_date >= str(datetime.now()):
                end_date = max_date
            else:
                end_date = datetime.now().strftime("%m/%d/%Y")

            x = datetime.strptime(end_date, "%m/%d/%Y") + timedelta(days=1)
            beg_date = x.strftime("%m/%d/%Y")

            # print("Check begin date = " + beg_date)
            # id, aa_num, fullname, enddate, aa
            fn_end_date_aa(id, found_aa_num, fullname,
                           end_date, 'PREV', EARL)
            ######################################################
            # Timing issue here, it tries the insert before the end date
            # entry is fully committed
            # Need to add something to make sure the end date is in place
            # or I get a duplicate error
            #########################################################
            q_check_enddate = '''
              SELECT aa_no, id, end_date 
              FROM aa_rec 
              WHERE aa_no = {0}
              AND aa = 'PREV'
              '''.format(found_aa_num)
            # print(q_check_enddate)
            q_confirm_enddate = do_sql(q_check_enddate, key=DEBUG, earl=EARL)

            v_enddate = q_confirm_enddate.fetchone()

            # print(v_enddate)

            if v_enddate is not None:
                fn_insert_aa(id, fullname, 'PREV', addr1, addr2, addr3, cty, st,
                        zp, ctry, beg_date, fn_format_phone(phone), EARL)
            else:
                # print("Failure on insert.  Could not verify enddate of previous")
                fn_write_error("Failure on insert.  Could not verify enddate "
                               "of previous")
            # print("An Address exists but does not match - end current, insert new")

        return "Success"

    except Exception as e:
        # print("Error in aarec.py, fn_archive_address, for ID " + id + ", Name "
        #       + fullname + " error = " + e.message)
        fn_write_error("Error in aarec.py, fn_archive_address, for ID " + str(id)
                       + ", Name " + fullname + " error = " + e.message)
###################################################
# SQL Functions
###################################################
# Query works 06/05/18
def fn_insert_aa(id, fullname, aa, addr1, addr2, addr3, cty, st, zp, ctry,
                 beg_date, phone,  EARL):
    # print("AA = " + aa + " ID = " + str(id)) + ", Name = " + fullname
    try:
        engine = get_engine(EARL)
        # print(beg_date)

        q_insert_aa = '''
            INSERT INTO aa_rec(id, aa, beg_date, peren, end_date, 
            line1, line2, line3, city, st, zip, ctry, phone, phone_ext, 
            ofc_add_by, cell_carrier, opt_out)
                          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
        q_ins_aa_args=(id, aa, beg_date, "N", "", addr1, addr2, addr3, cty, st,
                                        zp, ctry, phone, "", "HR", "", "")

        engine.execute(q_insert_aa,q_ins_aa_args)
        scr.write(q_insert_aa + '\n' + str(q_ins_aa_args));
        fn_write_log("Added " + addr1 + " to aa_rec for " + fullname + ", ID = " + str(id))
        # logger.info("Added archive address for " + fullname);
        # print(q_insert_aa)
        # print(q_ins_aa_args)
        # print("insert aa completed")

    except Exception as e:
        # print("Error in aarec.py, fn_insert_aa.  for ID " + str(id) + ", Name "
        #       + fullname + " Error = " + e.message)
        fn_write_error("Error in aarec.py, fn_insert_aa.  for ID " + str(id) + ", Name "
              + fullname + " Error = " + e.message)


# Query works 06/05/18
def fn_update_aa(id, aa, aanum, fllname, add1, add2, add3, cty, st, zip, ctry,
                 begdate, EARL):
    engine = get_engine(EARL)

    q_update_aa = '''
      UPDATE aa_rec 
      SET line1 = ?,
      line2 = ?,
      line3 = ?,
      city = ?,
      st = ?,
      zip = ?,
      ctry = ?  
      phone = ?
      WHERE aa_no = ?'''
    q_upd_aa_args=(add1, add2, add3, cty, st, zip,
                   ctry, fn_format_phone(phone), aanum)
    # logger.info("Updated address info in aa_rec table for " + fullname);
    fn_write_log("Updated " + add1 + " to aa_rec for " + fullname + ", ID = " + str(id))
    scr.write(q_update_aa + '\n' + str(q_upd_aa_args) + '\n');
    engine.execute(q_update_aa, q_upd_aa_args)
    #print(q_update_aa)
    #print(q_upd_aa_args)
    print("update aa completed")

# Query works 06/05/18
def fn_end_date_aa(id, aa_num, fullname, enddate, aa, EARL):
    engine = get_engine(EARL)

    try:
        q_enddate_aa = '''
          UPDATE aa_rec
          SET end_date = ?, aa = ?
          WHERE id = ?
          AND aa_no = ?'''
        q_enddate_aa_args=(enddate, aa, id, aa_num)
        engine.execute(q_enddate_aa, q_enddate_aa_args)
        # print("Log end date aa for " + fullname)
        fn_write_log("Added end date to address to aa_rec for " + fullname +
                      ", ID = " + str(id) + " aa_num = " + str(aa))
        # logger.info("Log end date aa_rec for " + fullname);
        scr.write(q_enddate_aa + '\n' + str(q_enddate_aa_args) + '\n')
        # print(q_enddate_aa)
        # print(q_enddate_aa_args)
        # print("end Date aa completed")
        return(1)
    except Exception as e:
        fn_write_error("Exception in aarec.py fn_end_date_aa, error = " + e.message)
        return (0)


#########################################################
# Specific function to deal with cell phone in aa_rec
#########################################################

def fn_set_cell_phone(phone, id, fullname, EARL):
    try:
        # Always get max date, in case insert has to be on same day
        q_check_begin = '''
             SELECT MAX(aa_rec.beg_date)
              FROM aa_rec 
              WHERE aa_rec.id = {0} AND aa_rec.aa = 'CELL' 
                  '''.format(id)
        # print(q_check_begin)

        sql_end = do_sql(q_check_begin, key=DEBUG, earl=EARL)
        beg_rslt = sql_end.fetchone()
        if beg_rslt[0] is None:
            # print('No existing begin date')
            begindate = datetime.now().strftime("%m/%d/%Y")
            enddate = datetime.now().strftime("%m/%d/%Y")
            # x = datetime.strptime(enddate, "%m/%d/%Y") + timedelta(days=1)
            # print("Begin Date = " + str(begindate))
            # print("End Date = " + str(enddate))
        # elif datetime.strftime(beg_rslt[0], "%m/%d/%Y") >= datetime.strftime(datetime.now(), "%m/%d/%Y"):
        else:
            x = beg_rslt[0]
            y = beg_rslt[0] + timedelta(days=1)
            enddate = x.strftime("%m/%d/%Y")
            begindate = y.strftime("%m/%d/%Y")
            # print("Begin Date = " + str(begindate))
            # print("End Date = " + str(enddate))

        q_check_cell = '''
            SELECT aa_rec.aa, aa_rec.id, aa_rec.phone, aa_rec.aa_no, 
            aa_rec.beg_date, aa_rec.end_date
            FROM aa_rec 
            WHERE aa_rec.id = {0} AND aa_rec.aa = 'CELL'  
            AND end_date is null
                '''.format(id)
        # print(q_check_cell)

        # print("Phone input var = " + phone)

        sql_cell = do_sql(q_check_cell, key=DEBUG, earl=EARL)
        cell_result = sql_cell.fetchone()
        if cell_result is None:
            # print("No Cell")

            fn_insert_aa(id, fullname, 'CELL',
                         "", "", "", "", "", "", "",
                         begindate, fn_format_phone(phone), EARL)
            return("New Cell Phone")

        elif cell_result[2] == phone:
            # print("Found phone = " + cell_result[2])
            return("No Cell Phone Change")

        else:
            # print("Found phone = " + cell_result[2])

            if cell_result[5] != '':
                # End date current CELL
                # print("Existing cell = " + cell_result[0])
                # print(datetime.strftime(end_rslt[0], "%m/%d/%Y"))
                # print(datetime.strftime(datetime.now(), "%m/%d/%Y"))
                fn_end_date_aa(id, cell_result[3], fullname, enddate, "CELL", EARL)
                fn_insert_aa(id, fullname, 'CELL', "", "", "", "", "", "", "",
                              begindate, fn_format_phone(phone), EARL)
                #print("New cell will be = " + phone)
                return ("Updated cell")
            # else:
            #     print("Already end dated")


    except Exception as e:
        # print("Error in aarec.py, fn_set_cell_phone, for ID " + id + ", Name "
        #       + fullname + " Error = " + e.message)
        fn_write_error("Error in aarec.py, fn_set_cell_phone, for ID " + str(id)
                       + ", Name " + fullname + " Error = " + e.message)
        return ""

#########################################################
# Specific function to deal with email in aa_rec
#########################################################
def fn_set_email(email, id, fullname, eml, EARL):
    try:
        # Have to get dates regardless, because begin date part of key,
        # cannot insert if date used
        q_check_begin = '''
          SELECT max(aa_rec.beg_date)
          FROM aa_rec 
          WHERE aa_rec.id = {0} AND aa_rec.aa = "{1}" 
          '''.format(id, eml)
        # print(q_check_begin)
        sql_begin = do_sql(q_check_begin, key=DEBUG, earl=EARL)
        beg_rslt = sql_begin.fetchone()
        # print("Beg Result = " + str(beg_rslt))

        # We will not update an existing email address.
        # If the email matches, no change
        # if no email, add new
        # if existing but different, end date the old, add new
        # Records of the same type cannot have the same start date.

        if beg_rslt[0] is None:
            begindate = datetime.now().strftime("%m/%d/%Y")
            # print("Set Email New Begin Date = " + begindate)
        else:
            # Normally, the begin date would be today.
            # If max begin date is already today, or future...
            # New begin date must be 1 day greater than last one
            y = beg_rslt[0] + timedelta(days=1)
            # print(str(y))
            # enddate = x.strftime("%m/%d/%Y")
            begindate = y.strftime("%m/%d/%Y")
            # print("Set Email Begin Date = " + str(begindate))

        q_check_email = '''
                      SELECT aa_rec.aa, aa_rec.id, aa_rec.line1, 
                      aa_rec.aa_no, aa_rec.beg_date 
                      FROM aa_rec
                      WHERE aa_rec.id = {0}
                      AND aa_rec.aa = "{1}" 
                      AND aa_rec.end_date IS NULL
                      '''.format(id, eml)
        # print(q_check_email)
        # logger.info("Select email info from aa_rec table");

        sql_email = do_sql(q_check_email, earl=EARL)

        # print("Begin Date = " + begindate)

        if sql_email is not None:
            email_result = sql_email.fetchone()
            # print(email_result)
            if email_result == None:
                print("New Email will be = " + email)
                # print("Begin Date = " + begindate)

                fn_insert_aa(id, fullname, eml,
                               email, "", "", "", "", "", "",
                               begindate, "", EARL)

                return("New email")
            elif email_result[2] == email:
                return("No email Change")
                # print("No Change")
            else:
                # End date current EMail

                print("Existing Email")
                      # + email_result[0])
                # print("Beg Date = " + str(begindate))
                # print("EMAIL = " + eml + ", " + email)
                enddate = datetime.now().strftime("%m/%d/%Y")
                fn_end_date_aa(id, email_result[3], fullname, enddate, eml, EARL)
                fn_insert_aa(id, fullname, eml, email, "", "", "", "", "",
                             "", begindate, "", EARL)

            return("Email updated")

    except Exception as e:
        # print("Error in aarec.py, fn_set_email, for for ID " + id + ", Name "
        #       + fullname + " Error = " + e.message)
        fn_write_error("Error in aarec.py, fn_set_email, for for ID " + str(id)
                       + ", Name " + fullname + ", Email " + email +
                       " Error = " + e.message)
        return ""

def fn_set_schl_rec(id, fullname, phone, ext, loc, room, EARL):
    engine = get_engine(EARL)

    q_check_schl = '''
      SELECT id, aa_no, beg_date, end_date, line1, line3, phone, phone_ext 
      FROM aa_rec 
      WHERE id = {0} 
      AND aa = "{1}"
      '''.format(id, "SCHL")
    #print(q_check_schl)
    try:
        sql_schl = do_sql(q_check_schl, earl=EARL)
        schl_result = sql_schl.fetchone()
        #print("result = " + str(schl_result))
        #print("Location = " + str(loc))
        #print("Room = " + str(room))


        location = str(loc) + " " + str(room)

        if schl_result is not None:
            if schl_result[4] == fullname and schl_result[5] == location \
                    and schl_result[6] == phone and schl_result[7] == ext:
                return("No Change in SCHL in aa_rec")
            else:
                q_update_schl = '''
                  UPDATE aa_rec
                  SET line1 = ?,
                  line3 = ?,
                  phone = ?,
                  phone_ext = ?
                  WHERE aa_no = ?
                '''
                q_upd_schl_args = (fullname, location, phone, ext, schl_result[1])
                # logger.info("update address info in aa_rec table");
                engine.execute(q_update_schl, q_upd_schl_args)
                fn_write_log("Update to SCHL record in aa_rec for " + fullname)
                scr.write(q_update_schl + '\n' + str(q_upd_schl_args) + '\n')
                #print(q_update_schl)
                #print(q_upd_schl_args)
                #print("update SCHL completed")
        else:
            # print("New SCHL rec will be added ")
            # add location and room?
            loc = ""
            carthphone = ""
            ext = ""
            q_insert_schl = '''
              INSERT INTO aa_rec(id, aa, beg_date, peren, end_date, line1, 
              line2, line3, city, st, zip, ctry, phone, phone_ext, ofc_add_by, 
              cell_carrier, opt_out)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            q_ins_schl_args = (id, "SCHL",  datetime.now().strftime("%m/%d/%Y"),
                "N", "", fullname, "", location, "", "", "", "", phone, ext,
                "HR", "", "")
            #print(q_insert_schl)
            #print(q_ins_schl_args)

            engine.execute(q_insert_schl, q_ins_schl_args)
            fn_write_log("Insert SCHL into aa_rec table for " + fullname)
            scr.write(q_insert_schl + '\n' + str(q_ins_schl_args) + '\n');
            #print("insert SCHL completed")

    except Exception as e:
        # print("Error in aarec.py, fn_set_schl_rec, Error = " + e.message)
        fn_write_error("Error in aarec.py, fn_set_schl_rec, for " + fullname
                       + ", ID = " + id + "Error = " + e.message)
    # finally:
    #     logging.shutdown()