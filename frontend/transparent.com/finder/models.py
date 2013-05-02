from django.db import models
from django.contrib.auth.models import User
from django.db.models import signals

# Create your models here.
class Module(models.Model):
    name = models.CharField(max_length=1024)
    source_name = models.CharField(max_length=1024)
    author = models.CharField(max_length=1024)
    timestamp = models.TimeField()
    up_score = models.IntegerField()
    down_score = models.IntegerField()
    backend_id = models.IntegerField()

class UserProfile(models.Model):
    user = models.OneToOneField(User)
    modules = models.ManyToManyField(Module)
    tracked_items = models.CommaSeparatedIntegerField(max_length=32000)

def create_profile(sender, instance, created, **kwargs):
    if created:
        profile = UserProfile(user=instance)
        profile.save()

signals.post_save.connect(create_profile, sender=User)