# django-djlabour
Django apps for interacting with the Automatic Data Processing (ADP) API.

# cronjob
00 01 * * * (cd /data2/python_venv/3.6/djlabour/ && . bin/activate && bin/python djlabour/bin/cc_adp_rec.py --database=cars 2>&1 | mail -s "[ADP] Doing something with data" adp@carthage.edu) >> /dev/null 2>&1
