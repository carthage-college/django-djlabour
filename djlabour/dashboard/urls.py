from django.conf.urls import url

from djlabour.dashboard import views

urlpatterns = [
    url(
        r'^$',
        views.home, name='home'
    ),
]
