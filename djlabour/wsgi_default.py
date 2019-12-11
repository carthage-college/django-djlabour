import os
import sys

# python
sys.path.append('/data2/python_venv/3.6/djlabour/lib/python3.6/')
sys.path.append('/data2/python_venv/3.6/djlabour/lib/python3.6/site-packages/')
# django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "djlabour.settings.staging")
os.environ.setdefault("PYTHON_EGG_CACHE", "/var/cache/python/.python-eggs")
os.environ.setdefault("TZ", "America/Chicago")
# informix
os.environ['INFORMIXSERVER'] = ''
os.environ['DBSERVERNAME'] = ''
os.environ['INFORMIXDIR'] = ''
os.environ['ODBCINI'] = ''
os.environ['ONCONFIG'] = ''
os.environ['INFORMIXSQLHOSTS'] = ''
os.environ['LD_LIBRARY_PATH'] = ''
os.environ['LD_RUN_PATH'] = ''

# wsgi
from django.core.wsgi import get_wsgi_application

# NOTE: remove the try/except in production
application = get_wsgi_application()
