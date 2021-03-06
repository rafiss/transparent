from django.conf.urls import patterns, include, url
from finder import views
#from django.contrib.auth.views import login, logout

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',
    # Examples:
    # url(r'^$', 'transparent.views.home', name='home'),
    # url(r'^transparent/', include('transparent.foo.urls')),
    (r'^$', views.index),
    (r'^search/$', views.search),
    (r'^product/([^/]+)/$', views.product),
    (r'^about/$', views.about),
    (r'^how_it_works/$', views.how_it_works),
    (r'^moduleAPI/$', views.moduleAPI),

	(r'^settings/$', views.settings),
	(r'^tracked_items/$', views.tracked_items),
	(r'^submit/$', views.submit),
    (r'^toggle/$', views.toggle_module),

    # for registration
    (r'^login/$', views.login),
    (r'^logout/$', views.logout),
    (r'^register/$', views.register),

    # price tracking
    (r'^track/$', views.track),
    (r'^stop_track/$', views.stop_track),

    (r'^chart/([^/]+)/$', views.chart),

    # module voting
    (r'^upvote/$', views.upvote),
    (r'^downvote/$', views.downvote),

    # Uncomment the admin/doc line below to enable admin documentation:
    url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
)
