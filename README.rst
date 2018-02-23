local_backup.py description
========================

This script is connecting to MySQL server, executes mysqldump and saves dumpfile to specified directory.
then, transfer dumpfiles to specified remote host.

config file path is <python3 lib directory>/site|dist-packages/daily_backup/config/backup.json

run following command to executes this scripts!(must be privileged user)
python3 <python3 lib directory>/dist-packages/daily_backup/local_backup.py

