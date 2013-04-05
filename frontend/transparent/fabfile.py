from fabric.api import env, run

def ec2():
    env.hosts = ['rafiss.com:55323']
    env.user = 'ec2-user'
    env.key_filename = '~/.ssh/transparentkey.pem'

def deploy():
    run('cd transparent && git pull')
    run('cp -r transparent/frontend/transparent /var/www/html')
    run('cd /var/www/html/transparent && python2 manage.py syncdb')
