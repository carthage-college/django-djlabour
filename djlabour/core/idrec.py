# django settings for script
from django.conf import settings

from djequis.core.utils import sendmail
from djzbar.utils.informix import get_engine
from djzbar.utils.informix import do_sql


# Imports for additional modules and functions written as part of this project
from djequis.adp.aarec import fn_archive_address
from djequis.adp.utilities import fn_validate_field, fn_write_log, fn_write_error, \
    fn_format_phone

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# write out the .sql file
scr = open("apdtocx_output.sql", "a")

#############################################
# Begin Processing
#############################################

def fn_process_idrec(carth_id, file_number, fullname, lastname, firstname,
        middlename, title, addr_line1, addr_line2, addr_line3, city, st,
        zip, ctry, ctry_cod, ss_no, phone, decsd, eff_date, EARL):
    print("Start ID Rec Processing")
    engine = get_engine(EARL)

    v_id = fn_validate_field(carth_id, "id", "id", "id_rec", "integer",
                              EARL)

    if v_id == 0:
            fn_write_log("ID not found in CX database.  ID = " + carth_id
                         + " Name = " + fullname)
            BODY = "ID not found in CX database for ID " + carth_id \
                   + " Name, " + fullname
            SUBJECT = "CX ID not found"
            # sendmail(
            #     settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
            #     BODY, SUBJECT)
    else:

        try:

            q_update_id_rec = ('''UPDATE id_rec SET fullname = ?, lastname = ?, 
                firstname = ?, middlename = ?, ss_no = ?, decsd = 'N', 
                upd_date = ?, ofc_add_by = 'HR' 
                WHERE id = ?''')

            q_update_id_args = (fullname, lastname, firstname, middlename, ss_no, eff_date,
                           carth_id)
            # print(q_update_id_rec)
            # print(q_update_id_args)
            fn_write_log("Update basic info in id_rec table for " + fullname +
                         ", ID = " + str(carth_id))
            scr.write(q_update_id_rec + '\n' + str(q_update_id_args) + '\n');
            # logger.info("Update id_rec table");
            engine.execute(q_update_id_rec, q_update_id_args)
        except Exception as err:
            # print(err.message)
            return (err.message)
            fn_write_error("Error in id_rec.py updating basic info.  Error = "
                           + err.message)
            # logger.error(err, exc_info=True)

        #########################################################
        # Title is a problem - most blank in ADP
        # To avoid overwriting, will need to validate and do
        # a separate update of the record
        try:


            if title is not None:
                x = title.replace(".", "")
                vTitle  = fn_validate_field(x.upper(), 'title', 'title', 'title_table',
                                         'char', EARL)
                # print("Title = " + str(vTitle))
                if vTitle is not None and vTitle != "":
                    q_update_title = ('''UPDATE id_rec SET title = ?
                                WHERE id = ?''')
                    q_update_title_args = (vTitle, carth_id)
                    fn_write_log("Update Title info in id_rec table for " + fullname +
                                 ", ID = " + str(carth_id))
                    scr.write(q_update_title + '\n' + str(q_update_title_args) + '\n');
                    # logger.info("Update id_rec table");
                    engine.execute(q_update_title, q_update_title_args)
            # else:
            #     print("No Title")

        except Exception as err:
            # print(err.message)
            return (err.message)
            fn_write_error("Error in id_rec.py updating title info.  Error = " + err.message)
            # logger.error(err, exc_info=True)

#########################################################

        # print("Country Code = " + str(len(ctry_cod)))

        try:
            # also need to deal with address changes
            # Search for existing address record
            if ctry_cod.strip() != '' and len(ctry_cod) > 0:
                cntry = fn_validate_field(ctry_cod, 'ctry', 'ctry', 'ctry_table', 'char', EARL)
                # print("Valid Country Code = " + cntry)

                # print(" In Check Address")
                q_check_addr = '''
                            SELECT id, addr_line1, addr_line2, addr_line3, city,
                                st, zip, ctry, phone
                            FROM id_rec
                            WHERE id = {0}
                                '''.format(carth_id)
                addr_result = do_sql(q_check_addr, key=DEBUG, earl=EARL)
                # scr.write(q_check_addr + '\n');
                row = addr_result.fetchone()
                if row is None:
                    fn_write_log("Data missing in idrec.py address function. \
                                                  Employee not in id rec for id "
                                 "number " + str(
                        carth_id))
                    # print("Employee not in id rec")
                elif str(row[0]) == '0' or str(row[0]) == '':  # No person in id rec? Should never happen
                    fn_write_log("Data missing in idrec.py address function. \
                                  Employee not in id rec for id number " + str(carth_id))
                    # print("Employee not in id rec")
                    BODY = "ID not found in CX database id_rec.py address " \
                           "routine for ID " + carth_id  + " Name, " + fullname
                    SUBJECT = "CX ID not found"
                    # sendmail(
                    #     settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
                    #     BODY, SUBJECT )

                # Update ID Rec and archive aa rec
                elif (row[1] != addr_line1
                    or row[2] != addr_line2
                    or row[3] != addr_line3
                    or row[4] != city
                    or row[5] != st
                    or row[6] != zip
                    or row[7] != ctry_cod):

                    # print("Update: no address match in ID_REC " + str(carth_id))  #

                    q_update_id_rec_addr = ('''UPDATE id_rec SET addr_line1 = ?,
                         addr_line2 = ?, addr_line3 = ?, city = ?, st = ?, zip = ?,
                         ctry = ?, aa = 'PERM', phone = ? WHERE id = ?''')
                    q_update_id_addr_args = (addr_line1, addr_line2, addr_line3, city, st,
                                            zip, cntry, fn_format_phone(phone), carth_id)

                    # print(q_update_id_rec_addr)
                    # print(q_update_id_addr_args)
                    fn_write_log("Update address info in id_rec table for " +
                                 fullname + ", ID = " + str(carth_id) +
                                 " address = " + addr_line1)
                    engine.execute(q_update_id_rec_addr, q_update_id_addr_args)
                    scr.write(q_update_id_rec_addr + '\n' + str(q_update_id_addr_args) + '\n')



                    #########################################################
                    # Routine to deal with aa_rec
                    #########################################################
                    # now check to see if address is a duplicate in aa_rec
                    # find max start date to determine what date to insert
                    # insert or update as needed
                    if row[1] is None:
                        # This should only happen on initial run, just need to
                        #  ignore the archive process if no address to archive
                        fn_write_log("Empty Address 1 in ID Rec - Nothing to "
                                     "archive")
                    elif row is not None:
                        # print("row[1] = " + row[1])
                        fn_archive_address(carth_id, fullname, row[1], row[2],
                                     row[3], row[4], row[5], row[6], row[7], phone,
                                           EARL)
                    else:
                        fn_write_log("Empty Address 1 in ID Rec - Nothing to "
                                     "archive")

                # else:
                #     print("No Change " + row[1])
            elif ctry_cod is None or len(ctry_cod) == 0:
                # print("invalid country code" + ctry_cod)
                fn_write_log("invalid country code" + ctry_cod)

        except Exception as err:
            # print(err.message)
            fn_write_error("Error in idrec.py for id " + carth_id
                           + ".  Error = " + err.message)



# logger.error(err, exc_info=True)
#     finally:
#          logging.shutdown()


