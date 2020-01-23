# from sqlalchemy import text
from djlabour.core.cc_adp_utilities import fn_format_phone, fn_convert_date
import datetime
from datetime import datetime
from time import strftime
from djimix.core.utils import get_connection, xsql

# django settings for script
from django.conf import settings


# This select statement should only return the most recent records
CX_VIEW_SQL = '''  SELECT
    file_no, carthage_id, lastname, firstname, middlename, 
    salutation, fullname, 
    pref_name, birth_date, gender, marital_status, 
    race,
    race_descr, hispanic, 
    race_id_method, personal_email, primary_addr_line1, 
    primary_addr_line2, 
    primary_addr_line3, primary_addr_city, primary_addr_st, 
    primary_addr_state, 
    primary_addr_zip, primary_addr_county, primary_addr_country, 
    primary_addr_country_code, primary_addr_as_legal, home_phone, 
    cell_phone, 
    work_phone, work_contact_phone, work_contact_email, 
    work_contact_notification, legal_addr_line1, legal_addr_line2, 
    legal_addr_line3, legal_addr_city, legal_addr_st, 
    legal_addr_state, 
    legal_addr_zip, legal_addr_county, legal_addr_country, 
    legal_addr_country_code, ssn, hire_date, hire_rehire_date, 
    rehire_date, 
    position_start_date, position_effective_date, 
    position_effective_end_date, 
    termination_date, position_status, status_effective_date, 
    status_effective_end_date, adjusted_service_date, 
    archived_employee, 
    position_id, primary_position, payroll_company_code, 
    payroll_company_name, 
    cip_code, worker_category_code, worker_category_descr, 
    job_title_code, 
    job_title_descr, home_cost_number_code, home_cost_number_descr, 
    job_class_code, job_class_descr, job_descr, job_function_code, 
    job_function_descr, room, bldg, bldg_name, 
    leave_of_absence_start_date, 
    leave_of_absence_return_date, home_depart_num_code, 
    home_depart_num_descr, 
    supervisor_id, supervisor_firstname, supervisor_lastname, 
    business_unit_code, business_unit_descr, reports_to_name, 
    reports_to_position_id, reports_to_associate_id, 
    employee_associate_id, 
    management_position, supervisor_flag, long_title
FROM
cc_adp_rec
where date_stamp in
    (select datestamp from
        (select carthage_id, fullname, termination_date,
         max(date_stamp) as datestamp
        from cc_adp_rec
    where (termination_date > "01/01/"||TO_CHAR(YEAR(TODAY)) 
            or termination_date is null)
        group by carthage_id, fullname, termination_date
        )
    )
	 '''


def Q_CC_ADP_VERIFY(row):
    # print(str(row[0]))
    # print("In Q_CC_ADP_VERIFY")
    # print(row)
    # print(row["race"])
    QUERY = '''
       SELECT file_no, carthage_id, lastname, firstname, 
       middlename, salutation, fullname, pref_name, 
       birth_date, 
       gender, marital_status, race, race_descr, hispanic, 
       race_id_method, personal_email, primary_addr_line1, 
       primary_addr_line2, primary_addr_line3, 
       primary_addr_city, 
       primary_addr_st, primary_addr_state, primary_addr_zip, 
       primary_addr_county, primary_addr_country, 
       primary_addr_country_code, primary_addr_as_legal, 
       home_phone, cell_phone, work_phone, work_contact_phone, 
       work_contact_email, work_contact_notification, 
       legal_addr_line1, legal_addr_line2, legal_addr_line3, 
       legal_addr_city, legal_addr_st, legal_addr_state, 
       legal_addr_zip, legal_addr_county, legal_addr_country, 
       legal_addr_country_code, ssn, hire_date, 
       hire_rehire_date, 
       rehire_date, position_start_date, 
       position_effective_date, 
       position_effective_end_date, termination_date, 
       position_status, status_effective_date, 
       status_effective_end_date, adjusted_service_date, 
       archived_employee, position_id, primary_position, 
       payroll_company_code, payroll_company_name, cip_code, 
       worker_category_code, worker_category_descr, 
       job_title_code, job_title_descr, home_cost_number_code, 
       home_cost_number_descr, job_class_code, 
       job_class_descr, 
       job_descr, job_function_code, job_function_descr, room, 
       bldg, bldg_name, leave_of_absence_start_date, 
       leave_of_absence_return_date, home_depart_num_code, 
       home_depart_num_descr, supervisor_id, 
       supervisor_firstname, 
       supervisor_lastname, business_unit_code, 
       business_unit_descr, reports_to_name, 
       reports_to_position_id, reports_to_associate_id, 
       employee_associate_id, management_position, 
       supervisor_flag, long_title, date_stamp  
       FROM cc_adp_rec 
       WHERE file_no = {0}
       AND carthage_id = {1}
       AND lastname = "{2}"
       AND firstname = "{3}"
       AND middlename = "{4}"
       AND salutation = "{5}"
       AND fullname = "{6}"
       AND pref_name = "{7}"
       AND NVL(birth_date, "") = "{8}"
       AND gender = "{9}"
       AND marital_status = "{10}"
       AND race = "{11}"
       AND race_descr = "{12}"
       AND hispanic = "{13}"
       AND race_id_method = "{14}"
       AND personal_email = "{15}"
       AND primary_addr_line1 = "{16}"
       AND primary_addr_line2 = "{17}"
       AND primary_addr_line3 = "{18}"
       AND primary_addr_city = "{19}"
       AND primary_addr_st = "{20}"
       AND primary_addr_state = "{21}"
       AND primary_addr_zip = "{22}"
       AND primary_addr_county = "{23}"
       AND primary_addr_country = "{24}"
       AND primary_addr_country_code = "{25}"
       AND primary_addr_as_legal = "{26}"
       AND home_phone = "{27}"
       AND cell_phone = "{28}"
       AND  work_phone = "{29}"
       AND work_contact_phone = "{30}"
       AND work_contact_email = "{31}"
       AND work_contact_notification = "{32}"
       AND legal_addr_line1 = "{33}"
       AND legal_addr_line2 = "{34}"
       AND legal_addr_line3 = "{35}"
       AND legal_addr_city = "{36}"
       AND legal_addr_st = "{37}"
       AND legal_addr_state = "{38}"
       AND legal_addr_zip = "{39}"
       AND legal_addr_county = "{40}"
       AND legal_addr_country = "{41}"
       AND legal_addr_country_code = "{42}"
       AND ssn = "{43}"
       AND NVL(hire_date, "") = "{44}"
       AND NVL(hire_rehire_date, "") = "{45}"
       AND NVL(rehire_date, "") = "{46}"
       AND NVL(position_start_date, "") = "{47}"
       AND NVL(position_effective_date, "") = "{48}"
       AND NVL(position_effective_end_date, "") = "{49}"
       AND NVL(termination_date, "") = "{50}"
       AND position_status = "{51}"
       AND NVL(status_effective_date, "") = "{52}"
       AND NVL(status_effective_end_date, "") = "{53}"
       AND NVL(adjusted_service_date,"") = "{54}"
       AND archived_employee = "{55}"
       AND position_id = "{56}"
       AND primary_position = "{57}"
       AND payroll_company_code = "{58}"
       AND payroll_company_name = "{59}"
       AND cip_code = "{60}"
       AND worker_category_code = "{61}"
       AND worker_category_descr = "{62}"
       AND job_title_code = "{63}"
       AND job_title_descr = "{64}"
       AND home_cost_number_code = "{65}"
       AND home_cost_number_descr = "{66}"
       AND job_class_code =  "{67}"
       AND job_class_descr =  "{68}"
       AND job_descr = "{69}"
       AND job_function_code = "{70}"
       AND job_function_descr = "{71}"
       AND room = "{72}"
       AND bldg = "{73}"
       AND bldg_name = "{74}"
       AND NVL(leave_of_absence_start_date,"") = "{75}"
       AND NVL(leave_of_absence_return_date,"") = "{76}"
       AND home_depart_num_code =  "{77}"
       AND home_depart_num_descr = "{78}"
       AND supervisor_id = "{79}"
       AND supervisor_firstname = "{80}"
       AND supervisor_lastname = "{81}"
       AND business_unit_code = "{82}"
       AND business_unit_descr = "{83}"
       AND reports_to_name = "{84}"
       AND reports_to_position_id = "{85}"
       AND reports_to_associate_id  = "{86}"
       AND employee_associate_id = "{87}"
       AND management_position = "{88}"
       AND supervisor_flag = "{89}"
       AND long_title = "{90}"            
       order by date_stamp
       limit 1
       '''.format(row["file_number"],
                  row["carth_id"],
                  str(row["last_name"]),
                  str(row["first_name"]),
                  str(row["middle_name"]), row["salutation"],
                  row["payroll_name"], row["preferred_name"],
                  fn_convert_date(row["birth_date"]),
                  (row["gender"][:1]),
                  row["marital_status"], row['race'],
                  (row["race_descr"][:24]),
                  row["ethnicity"], row["ethnicity_id_meth"],
                  row["personal_email"],
                  row["primary_address1"],
                  row["primary_address2"],
                  row["primary_address3"],
                  row["primary_city"],
                  row["primary_state_code"],
                  row["primary_state_descr"],
                  row["primary_zip"],
                  row["primary_county"],
                  row["primary_country"],
                  row["primary_country_code"],
                  (row["primary_legal_address"][:1]),
                  fn_format_phone(row["home_phone"]),
                  fn_format_phone(row["mobile_phone"]),
                  fn_format_phone(row["work_phone"]),
                  fn_format_phone(row["wc_work_phone"]),
                  row["wc_work_email"],
                  (row["use_work_for_notification"][:1]),
                  row["legal_address1"], row["legal_address2"],
                  row["legal_address3"], row["legal_city"],
                  row["legal_state_code"],
                  row["legal_state_description"],
                  row["legal_zip"],
                  row["legal_county"],
                  row["legal_country"],
                  row["legal_country_code"], row["ssn"],
                  fn_convert_date(row["hire_date"]),
                  fn_convert_date(row["hire_rehire_date"]),
                  fn_convert_date(row["rehire_date"]),
                  fn_convert_date(row["pos_start_date"]),
                  fn_convert_date(row["pos_effective_date"]),
                  fn_convert_date(
                      row["pos_effective_end_date"]),
                  fn_convert_date(row["termination_date"]),
                  row["position_status"],
                  fn_convert_date(
                      row["status_effective_date"]),
                  fn_convert_date(row["status_eff_end_date"]),
                  fn_convert_date(row["adj_service_date"]),
                  row["archived"], row["position_id"],
                  row["primary_position"],
                  row["payroll_comp_code"],
                  row["payroll_comp_name"],
                  row["cip"],
                  row["worker_cat_code"],
                  row["worker_cat_descr"],
                  row["job_title_code"],
                  row["job_title_descr"],
                  row["home_cost_code"],
                  row["home_cost_descr"],
                  row["job_class_code"],
                  row["job_class_descr"],
                  row["job_description"],
                  row["job_function_code"],
                  row["job_function_description"],
                  row["room_number"],
                  row["location_code"],
                  row["location_description"],
                  fn_convert_date(row["leave_start_date"]),
                  fn_convert_date(row["leave_return_date"]),
                  row["home_dept_code"],
                  row["home_dept_descr"],
                  row["supervisor_id"],
                  row["supervisor_fname"],
                  row["supervisor_lname"],
                  row["business_unit_code"],
                  row["business_unit_descr"],
                  row["reports_to_name"],
                  row["reports_to_pos_id"],
                  row["reports_to_assoc_id"],
                  row["employee_assoc_id"],
                  row["management_position"],
                  row["supervisor_flag"],
                  row["long_title"]
                  )

    return(QUERY)

def INS_CC_ADP_REC(row, EARL):
    # print("Start Insert Query")

    """Informix is very picky about the timestamp format"""
    formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + \
                  datetime.now().strftime("%f")[:5]
    # print(formatted_time)

    q_cc_adp_rec = ("INSERT INTO cc_adp_rec (file_no, \
        carthage_id, lastname, firstname, middlename, \
        salutation, fullname, pref_name, birth_date, \
        gender,  marital_status, race, race_descr, \
        hispanic, race_id_method, personal_email, \
        primary_addr_line1, primary_addr_line2, \
        primary_addr_line3, primary_addr_city, \
        primary_addr_st, primary_addr_state, \
        primary_addr_zip, primary_addr_county,\
        primary_addr_country, primary_addr_country_code,\
        primary_addr_as_legal, home_phone, \
        cell_phone, work_phone, work_contact_phone, \
        work_contact_email, work_contact_notification, \
        legal_addr_line1, legal_addr_line2, \
        legal_addr_line3, legal_addr_city, \
        legal_addr_st, legal_addr_state, \
        legal_addr_zip, legal_addr_county, \
        legal_addr_country, legal_addr_country_code, \
        ssn, hire_date, hire_rehire_date, rehire_date, \
        position_start_date, position_effective_date, \
        position_effective_end_date, termination_date, \
        position_status, status_effective_date, \
        status_effective_end_date, adjusted_service_date, \
        archived_employee, position_id, primary_position, \
        payroll_company_code, payroll_company_name, \
        cip_code, worker_category_code, worker_category_descr, \
        job_title_code, job_title_descr, home_cost_number_code, \
        home_cost_number_descr, job_class_code, job_class_descr, \
        job_descr, job_function_code, \
        job_function_descr, room, bldg, bldg_name, \
        leave_of_absence_start_date, \
        leave_of_absence_return_date, \
        home_depart_num_code, home_depart_num_descr, \
        supervisor_id, supervisor_firstname, supervisor_lastname, \
        business_unit_code, business_unit_descr, reports_to_name, \
        reports_to_position_id, reports_to_associate_id, \
        employee_associate_id, management_position, \
        supervisor_flag, long_title, date_stamp) \
        VALUES \
        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    # print(q_cc_adp_rec)
    cc_adp_args = (row["file_number"], row["carth_id"],
        row["last_name"],
        row["first_name"], row["middle_name"],
        row["salutation"],
        row["payroll_name"], row["preferred_name"],
        fn_convert_date(row["birth_date"]),
        (row["gender"][:1]),
        row["marital_status"], row["race"],
        (row["race_descr"][:24]),
        row["ethnicity"],
        row["ethnicity_id_meth"], row["personal_email"],
        row["primary_address1"], row["primary_address2"],
        row["primary_address3"], row["primary_city"],
        row["primary_state_code"],
        row["primary_state_descr"],
        row["primary_zip"], row["primary_county"],
        row["primary_country"], row["primary_country_code"],
        (row["primary_legal_address"][:1]),
        fn_format_phone(row["home_phone"]),
        fn_format_phone(row["mobile_phone"]),
        fn_format_phone(row["work_phone"]),
        fn_format_phone(row["wc_work_phone"]),
        row["wc_work_email"],
        (row["use_work_for_notification"][:1]),
        row["legal_address1"],
        row["legal_address2"], row["legal_address3"],
        row["legal_city"], row["legal_state_code"],
        row["legal_state_description"], row["legal_zip"],
        row["legal_county"], row["legal_country"],
        row["legal_country_code"], row["ssn"],
        fn_convert_date(row["hire_date"]),
        fn_convert_date(row["hire_rehire_date"]),
        fn_convert_date(row["rehire_date"]),
        fn_convert_date(row["pos_start_date"]),
        fn_convert_date(row["pos_effective_date"]),
        fn_convert_date(row["pos_effective_end_date"]),
        fn_convert_date(row["termination_date"]),
        row["position_status"],
        fn_convert_date(row["status_effective_date"]),
        fn_convert_date(row["status_eff_end_date"]),
        fn_convert_date(row["adj_service_date"]),
        row["archived"], row["position_id"],
        row["primary_position"], row["payroll_comp_code"],
        row["payroll_comp_name"], row["cip"],
        row["worker_cat_code"], row["worker_cat_descr"],
        row["job_title_code"], row["job_title_descr"],
        row["home_cost_code"], row["home_cost_descr"],
        row["job_class_code"], row["job_class_descr"],
        row["job_description"], row["job_function_code"],
        row["job_function_description"], row["room_number"],
        row["location_code"], row["location_description"],
        fn_convert_date(row["leave_start_date"]),
        fn_convert_date(row["leave_return_date"]),
        row["home_dept_code"], row["home_dept_descr"],
        row["supervisor_id"], row["supervisor_fname"],
        row["supervisor_lname"],
        row["business_unit_code"].zfill(3),
        row["business_unit_descr"], row["reports_to_name"],
        row["reports_to_pos_id"],
        row["reports_to_assoc_id"],
        row["employee_assoc_id"],
        row["management_position"],
        row["supervisor_flag"], row["long_title"], formatted_time )

    # print(cc_adp_args)

    connection = get_connection(EARL)
    with connection:
        cur = connection.cursor()
        cur.execute(q_cc_adp_rec, cc_adp_args)

    # scr.write(q_cc_adp_rec + '\n' + str(cc_adp_args) + '\n');
    # fn_write_log("Inserted data into cc_adp_rec table for "
    #              + row["payroll_name"] + " ID = "
    #              + row["carth_id"]);
    # print("Inserted data into cc_adp_rec table for "
    #        + row["payroll_name"] + " ID = "
    #        + row["carth_id"]);
