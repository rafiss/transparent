from django_cron import CronJobBase, Schedule
from finder.models import Module
from transparent.settings import BACKEND_URL
import json, urllib2

class FetchModulesJob(CronJobBase):
    RUN_EVERY_MINS = 60 * 24 # every day

    schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
    code = 'finder.fetch_modules'    # a unique code

    def do(self):
        resp = urllib2.urlopen(BACKEND_URL + '/modules')
        modules = json.loads(resp.read())

        for bid, info in modules.items():
            exists = Module.objects.get(backend_id=bid)
            if not exists:
                m = Module(backend_id=bid, name=info['name'], source_name=info['source'], author="")
                m.save()
