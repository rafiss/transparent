from fabric.api import env, run, cd

def ec2():
    env.hosts = ['rafiss.com:55323']
    env.user = 'ec2-user'
    env.key_filename = '~/.ssh/transparentkey.pem'
    env.project_root = '/var/www/transparent.com/'

def deploy():
    with cd('transparent'):
        run('git pull')
    run('cp -r transparent/frontend/transparent.com/* ' +env.project_root)

def deploy_db():
    with cd(env.project_root):
        run('python2 manage.py syncdb')

def deploy_static():
    with cd(env.project_root):
        run('python2 manage.py collectstatic -v0 --noinput')

