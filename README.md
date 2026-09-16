# cre



# set up local polstgres
## Update your package list
>>> sudo apt update

## Install PostgreSQL and its additional utilities
>>>sudo apt install postgresql postgresql-contrib

Start the service:  
>>>sudo service postgresql start  
Check the status:  
>>>sudo service postgresql status  
Stop the service:  
>>>sudo service postgresql stop  


 Connect via the Terminal (psql)  
>>>sudo -u postgres psql


```
sql
-- Set a password for the default 'postgres' user  
ALTER USER postgres WITH PASSWORD 'your_secure_password';

-- Create a fresh database for your project  
CREATE DATABASE my_project_db;
```