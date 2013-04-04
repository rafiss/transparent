from fabric.api import env, run

def ec2():
    env.hosts = ['rafiss.com:55323']
    env.user = 'ec2-user'
    env.key_filename = '~/.ssh/rafisskey.pem'

def deploy():
    run('cd transparent && git pull')
    run('cp -r frontend/transparent /var/www/html')
