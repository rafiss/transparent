from django.conf.urls import patterns, include, url
from finder.views import index, hello, register, profile
from django.contrib.auth.views import login, logout

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'transparent.views.home', name='home'),
    # url(r'^transparent/', include('transparent.foo.urls')),
    (r'^$', index),
    (r'^hello/$', hello),
    (r'^login/$',  login),
    (r'^logout/$', logout, {'template_name' : 'index.html'}),
    (r'^profile/$',  profile),
    (r'^register/$', register),

    # Uncomment the admin/doc line below to enable admin documentation:
    url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
