# Data Pipeline (DataLake to DataWarehouse)
Data Pipeline is a service that helps you reliably move data between DataLake (Redshift cluster 1) to DataWarehouse (REDSHIFT cluster 2) at specified intervals.
We maintain a process_table where we store which all tables to sync in the clusters and the last time they were synced successfully. 
Data Pipeline perform following functions.
1. Reads data from processTable in cluster 2.
2. Read data for following tables from cluster 1. We run two queries, first to get new records, and other to get updated records.
3. Delete the updated rows (old data) from cluster 2 table.
4. Merge new records and updated records in a single data structure.
4. Create a corresponding csv file and upload to s3.
5. Copy the data to redshift cluster 2 table.
6. Update process table and process table snapshot in cluster 2.
7. One needs to manually create a table in cluster 2 if any new table is added and run a command on terminal to add a new entry in the process table with the desired info.

# Assumptions
1. Cluster 1 is our source of truth.
2. We never delete a column from a table; only add.
3. All tables will be synced once a day.

# Future Improvements
1. Automate adding new table process, i.e., script should check if table exists in cluster 2 or not and hence create.
2. Check if number of columns in cluster 1 table and cluster 2 table are equal or not; if not, alter table and add new column.

# Architecture
- Have a script in nodejs hosted on aws lambda which reads from process_table.
- This will in turn another lambda parallelly (5 instances at a time) that performs the next steps.

## Edge Cases

### First time syncing of tables
Lambda has a limit of 10GB memory and 15mins timeout. Can re-invoke the lambda in case of timeout and process in batches.

### In case of failure/error
- In case a process fails, will retry once.
- If it again fails, will log the error and move forward.
- Next time when it runs, will fetch records as per the sync_time in process_table. If fails again, manual intervention needed!