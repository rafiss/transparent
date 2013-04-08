from fabric.api import env, run

def ec2():
    env.hosts = ['rafiss.com:55323']
    env.user = 'ec2-user'
    env.key_filename = '~/.ssh/transparentkey.pem'

def deploy():
    run('cd transparent && git pull')
    run('cp -r transparent/frontend/transparent.com/* /var/www/transparent.com/')

def deploy_db():
    run('cd transparent && git pull')
    run('cp -r transparent/frontend/transparent.com/* /var/www/transparent.com/')
    run('cd /var/www/transparent.com && python2 manage.py syncdb')

def deploy_static():
    run('cd transparent && git pull')
    run('cp -r transparent/frontend/transparent.com/* /var/www/transparent.com/')
    run('cd /var/www/transparent.com && python2 manage.py collectstatic')

