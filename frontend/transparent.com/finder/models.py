from django.db import models
from django.contrib.auth.models import User
from django.db.models import signals

# Create your models here.
class Module(models.Model):
    name = models.CharField(max_length=1024)
    source_name = models.CharField(max_length=1024)
    author = models.CharField(max_length=1024)
    timestamp = models.DateField(auto_now_add=True)
    up_score = models.IntegerField(default=0)
    down_score = models.IntegerField(default=0)
    backend_id = models.TextField()

    def __unicode__(self):
        return u'id:{0} name:{1}'.format(self.backend_id, self.name)

class Product(models.Model):
    gid = models.CharField(max_length=1024)
    price = models.IntegerField()
    name = models.TextField()

    def __unicode__(self):
        return u'product {0}'.format(self.gid)

class UserProfile(models.Model):
    user = models.OneToOneField(User)
    modules = models.ManyToManyField(Module)
    products = models.ManyToManyField(Product, through='Track')
    up_modules = models.ManyToManyField(Module, related_name='up+')
    down_modules = models.ManyToManyField(Module, related_name='down+')

    def __unicode__(self):
        return u'{0} profile'.format(self.user.username)

class Track(models.Model):
    userprofile = models.ForeignKey(UserProfile)
    product = models.ForeignKey(Product)
    threshold = models.IntegerField()

def create_profile(sender, instance, created, **kwargs):
    if created:
        profile = UserProfile(user=instance)
        profile.save()

signals.post_save.connect(create_profile, sender=User)
