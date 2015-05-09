# couchbase-utility
Command for yii2 that you can use to import tables from mysql into couchbase server, and also synch data in your mysql database into couchbase server


Usage
-----
The command can be used in to scenarios :

1- first scenario : to import a table into couchbase :
	
    -> php yii couchbase/import sqlConnection, cbConnection, table, bucket_name

sqlConnection : is the name of database connection component where the table is

cbConnection : is the name of couchbase connection component, you can use this https://github.com/MiladAlshomary/couchbase-yii2 

table : name of the table to be imported

bucket_name : name of the bucket to import to


2- second scenario : to run cron job to keep pulling updated records from table into couchabase server.

	-> php yii couchbase/sync sqlConnection, cbConnection, table, sync_field
    

    

Instructions to install couchbase server and PHP client on CentOS/Redhat
-----

Install the couchbase php client:
 
<pre>
sudo nano /etc/yum.repos.d/couchbase.repo
</pre>

copy:
<pre> 
[couchbase]
enabled = 1
name = Couchbase package repository
baseurl = get url from http://docs.couchbase.com/developer/c-2.4/download-install.html
gpgcheck = 1
gpgkey = http://packages.couchbase.com/rpm/couchbase-rpm.key
</pre>

save file

<pre> 
yum install libcouchbase2-core 
yum install libcouchbase-devel 
yum install libcouchbase2-bin 
yum install libcouchbase2-libevent
pecl install couchbase
</pre>

Install couchbase-server:
<pre>
wget get url from http://www.couchbase.com/nosql-databases/downloads
sudo rpm --install couchbase-server-enterprise-3.0.2-centos6.x86_64.rpm
</pre>

