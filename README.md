Transparent
===========
An Open Price Tracking Platform
-------------------------------

Transparent is an open, extensible product price tracker.

###Frontend Development Info

On your computer you need

    python-pip

With pip install:

    django
    MySQL-python
    fabric

To run the site locally:

    $ python manage.py runserver

To deploy the current HEAD of the github master branch to the EC2 instance:

    $ cd frontend/transparent
    $ fab ec2 deploy

