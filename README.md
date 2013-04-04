Transparent
===========
An Open Price Tracking Platform
-------------------------------

Transparent is an open, extensible product price tracker.

###Frontend Development Info

On your computer you need
    python-pip
    mysql

With pip install:
    django
    MySQL-python
    fabric

To run the site locally:
    $ python manage.py runserver

To create a local development database:
(I plan to set up a remote dev DB so this becomes unnecessary)
    $ mysql -u root -p
    Enter password: (USE PASSWORD YOU DEFINED WHEN SETTING UP MYSQL)
    Welcome to the MySQL monitor.  Commands end with ; or \g.
    Your MySQL connection id is 1
    Server version: 5.0.51a-3ubuntu5.1 (Ubuntu)

    Type 'help;' or '\h' for help. Type '\c' to clear the buffer.

    mysql> CREATE DATABASE transparent_db;
    Query OK, 1 row affected (0.01 sec)

    mysql> GRANT ALL ON transparent_db.* TO 'transparent_user'@'localhost' IDENTIFIED BY 'trans333';
    Query OK, 0 rows affected (0.03 sec)

    mysql> quit
    Bye
    
To deploy the current HEAD of the master branch on github:
    $ cd frontend/transparent
    $ fab ec2 deploy

