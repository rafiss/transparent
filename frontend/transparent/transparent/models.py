from django.db import models

class User(models.Model):
    username = models.CharField(max_length=20)
    password = models.CharField(max_length=20)

    def __unicode__(self):
        return self.username

class UserModules(models.Model):
    user = models.ForeignKeys(User)
    modulename = models.CharField(max_length=200)
    moduleID = IntegerField(default=0)

    def __unicode__(self):
        return self.modulename
