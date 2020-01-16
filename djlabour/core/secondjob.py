# django settings for script
from django.conf import settings
from djzbar.utils.informix import do_sql
from djzbar.utils.informix import get_engine
from djequis.core.utils import sendmail


# Imports for additional modules and functions written as part of this project
from djequis.adp.utilities import fn_validate_field, fn_write_log, \
    fn_write_error, fn_needs_update

DEBUG = settings.INFORMIX_DEBUG

# set up command-line options
desc = """
    Upload ADP data to CX
"""

# write out the .sql file
scr = open("apdtocx_output.sql", "a")

def fn_process_second_job(carthid, workercatcode, pcnaggr, jobtitledescr,
                          positionstart, poseffectend, jobfunctioncode,
                          supervisorid, rank, fullname, EARL):
    engine = get_engine(EARL)

    print("In Second Job")
    print(pcnaggr)

    try:
        ##############################################################
        # Differs from regular job rec routine
        # Most of the information has already been validated since this
        # job is in the same record row as the primary
        # Same supervisor as primary job
        # Sane homedept code for the EMPLOYEE
        # Same business unit code for the EMPLOYEE
        # Same room and building, but probably can ignore since not primary position
        # Same worker category code and description, no validation needed
        # Payroll company code COULD be different, but shouldn't be
        # Job Function Code should be the same
        # Split PCN_Aggr (Home Cost Number) into separate components
        # first I should determine if this is an insert or update - see if
        #   pcn_aggr is in the pos_table
        # see if pcn code returns a position number
        # validate function code in the func_table (Position 2)
        # Place dept number in func_area field in position table
        # Must validate Division, Dept
        # NOT assuming custom field is correct, will notify if no matches
        # use PCN Codes to tie employee to job number
        ##############################################################

        ###############################################################
        # disassmble pcn code into parts JobType, Div, Dept, Job code
        ###############################################################
        print(pcnaggr)
        len = pcnaggr.__len__()
        pos1 = pcnaggr.find('-', 0)
        paycode = pcnaggr[:pos1]
        pos2 = pcnaggr.find('-', pos1 + 1, len)
        div = pcnaggr[pos1 + 1:pos2]
        pos3 = pcnaggr.find('-', pos2 + 1, len)
        dept = pcnaggr[pos2 + 1:pos3]
        jobnum = pcnaggr[pos3 + 1:len]

        spvrID = supervisorid[3:10]

        ###############################################################
        # Use PCN Agg to find TPos FROM position rec
        ###############################################################
        v_tpos = fn_validate_field(pcnaggr,"pcn_aggr","tpos_no",
                        "pos_table","char",EARL)

        # if v_tpos == 0 or v_tpos == "" or len(str(v_tpos)) == 0:
        if v_tpos == "":
        # if v_tpos == None or len(str(v_tpos)) == 0:
            print("Position not valid")
            raise ValueError()
        else:
            print("Validated t_pos = " + str(v_tpos))

        ##############################################################
        # validate hrpay, values in this table should not change without
        # a project request as they affect a number of things
        ##############################################################
        hrpay_rslt = fn_validate_field(paycode,"hrpay","hrpay", "hrpay_table",
                                       "char", EARL)
        if hrpay_rslt != '':
            print('Validated HRPay Code = ' + str(hrpay_rslt) + '\n')
        else:
            print('Invalid Payroll Company Code ' + str(paycode) + '\n')
            fn_write_log('Data Error in secondjob.py - Invalid Payroll Company \
                Code for secondary job ' + str(paycode) + '\n');

        func_code = fn_validate_field(dept,"func","func", "func_table",
                                    "char",EARL)
        if func_code != '':
            print('Validated second job func_code = ' + dept + '\n')
        else:
            print('Invalid Function Code ' + dept + '\n')
            fn_write_log('Data Error in second job.py - Invalid Function \
                Code = ' + dept + '\n');

        ##############################################################
        # Need some additional info from existing cx records
        # ADP does not have field for second job title
        ##############################################################
        q_get_title = '''
          SELECT distinct job_rec.job_title  
          FROM job_rec Where tpos_no = {0}'''.format(v_tpos)
        #print(q_get_title)
        sql_title = do_sql(q_get_title, key=DEBUG, earl=EARL)
        titlerow = sql_title.fetchone()
        if titlerow is None:
            print("Job Title Not found for tpos " + v_tpos)
            jr_jobtitle = ""
            fn_write_log('Job Title Not found for secondary job for tpos ' +
                         str(v_tpos) + '\n');
        else:
            jr_jobtitle = titlerow[0]
            print("Job Title = " + jr_jobtitle)

        ##############################################################
        # validate the position, division, department
        ##############################################################
        # print("....Deal with division...")
        hrdivision = fn_validate_field(div,"hrdiv","hrdiv", "hrdiv_table",
                                       "char",EARL)

        if hrdivision == None or hrdivision == "":
            print("HR Div not valid - " + div)

        # print("....Deal with department...")
        hrdepartment = fn_validate_field(dept,"hrdept","hrdept",
                    "hrdept_table", "char",EARL)
        #print(hrdepartment)
        if hrdepartment==None or hrdepartment=="":
            print("HR Dept not valid - " + dept)
            fn_write_log('Data Error in second job.py - HR Dept not valid ' +
                         dept + '\n');

        # ##############################################################
        # If job rec exists for employee in job_rec -update, else insert
        # ##############################################################

        q_check_exst_job = '''
        select job_rec.tpos_no, pos_table.pcn_aggr, job_no
        from job_rec, pos_table
        where job_rec.tpos_no = pos_table.tpos_no
        and job_rec.title_rank =  {0}
        and job_rec.id = {1}
        and (job_rec.end_date is null
        or job_rec.end_date > TODAY)
        '''.format(rank, carthid)
        # print(q_check_exst_job)
        sql_exst_job = do_sql(q_check_exst_job, key=DEBUG, earl=EARL)
        exst_row = sql_exst_job.fetchone()
        if exst_row is None:
            print("No Existing secondary jobs")
        else:
            if exst_row[1] != pcnaggr:
                print("Search job = " + pcnaggr)
                print("Existing job = " + exst_row[1])
                q_end_job = '''update job_rec set end_date = ?
                  where id = ? and job_no = ?
                  '''
                q_end_job_args = (datetime.now().strftime("%m/%d/%Y"),
                                  carthid, exst_row[2])
                print(q_end_job)
                print(q_end_job_args)
                engine.execute(q_end_job, q_end_job_args)

        q_get_job = '''
          SELECT job_no
          FROM job_rec
          WHERE tpos_no = {0}
          AND id = {1}
          AND (end_date IS null
          or end_date > TODAY)
          '''.format(v_tpos,carthid,positionstart)
        # print(q_get_job)
        sql_job = do_sql(q_get_job, key=DEBUG, earl=EARL)
        jobrow = sql_job.fetchone()
        if jobrow is None:
            print("Job Number not found in job rec")
            #  if no record, no duplicate
            #     insert
            q_ins_job = '''
              INSERT INTO job_rec
              (tpos_no, descr, bjob_no, id, hrpay, supervisor_no, hrstat, 
              egp_type, hrdiv, hrdept, comp_beg_date, comp_end_date, beg_date, 
              end_date, active_ctrct, ctrct_stat, excl_srvc_hrs, excl_web_time, 
              job_title, title_rank, worker_ctgry)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
               ?, ?)'''
            q_ins_job_args = (v_tpos, jr_jobtitle, 0, carthid, paycode, 0,
                              jobfunctioncode, 'R', div, func_code, None, None,
                              datetime.now().strftime("%m/%d/%Y"),
                              None if poseffectend == '' else poseffectend, 'N',
                              'N/A', 'N', 'N', jobtitledescr, rank,
                              workercatcode)
            # print(q_ins_job + str(q_ins_job_args))
            print("New Second Job Record for " + fullname + ', id = '
                  + str(carthid))
            fn_write_log('New secondary Job Record for ' + fullname +
                         ', id = ' + str(carthid) + '\n');
            engine.execute(q_ins_job, q_ins_job_args)
            scr.write(q_ins_job + '\n' + str(q_ins_job_args) + '\n');
        else:
            # jobrow = sql_job.fetchone()
            #print('valid job found = ' + str(jobrow[0]))
            #print('v_tpos = ' + str(v_tpos) )
            q_upd_job = '''
                UPDATE job_rec SET descr = ?,
                id = ?, hrpay = ?, supervisor_no = ?,
                hrstat = ?, hrdiv = ?, hrdept = ?,
                beg_date = ?, end_date = ?,
                job_title = ?,
                title_rank = ?, worker_ctgry = ?
                WHERE job_no = ?'''
            q_upd_job_args = (jr_jobtitle, carthid, paycode, 0,
                              jobfunctioncode, div, func_code,
                              datetime.now().strftime("%m/%d/%Y"),
                              None if poseffectend == '' else poseffectend,
                              jobtitledescr, rank, workercatcode, jobrow[0])
            # print(q_upd_job)
            #print(q_upd_job_args)
            engine.execute(q_upd_job, q_upd_job_args)
            scr.write(q_upd_job + '\n' + str(q_upd_job_args) + '\n');
            print("Update Second Job Record for " + fullname + ', id = '
                  + str(carthid))
            fn_write_log('Update Job Record for ' + fullname + ', id = '
                         + str(
                carthid) + '\n');
        return 1
        ##############################################################
        # Faculty Qualifications - This will go into facqual_rec...
        # and qual_table - No longer part of Job Title
        # Probably not in scope as these titles do not affect pay
        ##############################################################
    except ValueError:
        print("Position not valid for PCN_AGGR " + pcnaggr)
        SUBJECT = '[APD To CX Application] Data Error'
        BODY = "The Home Cost Number Code is not valid for secondary job.  " \
               "Code = " + pcnaggr
        # sendmail(
        #     settings.ADP_TO_EMAIL, settings.ADP_FROM_EMAIL,
        #     BODY, SUBJECT
        # )
        fn_write_log("The Home Cost Number Code is not valid for secondary "
                     "job.  Code = " + pcnaggr)

    except Exception as e:
        print("Error in second job for " + fullname + " ID = "
                       + carthid + " Error = "  + e.message)
        fn_write_error("Error in second job for " + fullname + " ID = "
                       + carthid + " Error = "  + e.message)

        return 0

