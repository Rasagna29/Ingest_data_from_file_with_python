# ingest_data_from_file_with_python

### Note: 
This project is designed for educational purposes. Redistribution, modification, or copying of this project is strictly prohibited.

### Description:
This demonstrates how to set up a DuckDB database and interact with it using Python for data ingestion. It is similar to what we did in [https://github.com/Rasagna29/ingest_data_with_python](ingest_data_with_python), but the main difference lies in the method of ingesting data into the DuckDB database. In the previous approach, we ingested data by running SQL queries from Python scripts. In this version, the assumption is that data is made available as CSV files in a directory. The script monitors the directory in real time to consume new files, process them, and ingest the data into the database.

We attempted to search the directory in two ways. The first method uses a custom Python implementation called watch_directory in /scripts/watch_directory_custom_script.py. When executed, it checks if the directory (/files/watch_directory_uploads) contains new files. If a new file is found and the data passes the validation check, it is processed and ingested into the database. The script then sleeps for 30 seconds before rerunning the check to see if any new files are available. To test this, upload the order_data_202412110923.csv file only to /files/watch_directory_uploads and allow the watch_directory_custom_script.py script to identify and process it. The script will print "Search is over, the next search is in 30 seconds." If you upload another file within those 30 seconds, the script will identify and process it once the next cycle starts.

In contrast, the /scripts/watchdog_python_library.py method works using an observer pattern. As soon as a new file is uploaded to /files/watchdog_uploads, it is immediately picked up for processing, and the data is ingested without waiting for a time interval as in the first method.

### Clone the Repository
Clone the repository to your local machine:
git clone [https://github.com/Rasagna29/ingest_data_from_file_with_python.git](https://github.com/Rasagna29/ingest_data_from_file_with_python.git)

## Installation and Setup

Follow these steps to set up the environment and tools: 

### 1. Install Homebrew (if not already installed)
Homebrew is a package manager for macOS. To install it, open the terminal and run:
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

### 2. Install Python (if not already installed)
Install it using Homebrew:

brew install python

### 3. Install virtual Environment  (if not already installed)
Note: A virtual environment isolates project dependencies, preventing conflicts and ensuring consistent setups across different projects.
pip install virtualenv

### 4. Set Up the Project Directory
mkdir ingest_data_from_file_with_python/

cd ingest_data_from_file_with_python/

### 5. Create and Activate a Virtual Environment
To keep the dependencies isolated, create and activate a Python virtual environment:

##### To create Python virtual environment  (this is done for each project)
python3 -m venv venv

##### To activate Python virtual environment
#### For bash/zsh users:
source venv/bin/activate  
#### For fish shell users:
source venv/bin/activate.fish

### 6. Install Required Python Libraries
Once your virtual environment is activated, install the necessary libraries for DuckDB, Pandas, Apache Airflow and Watchdog

##### pip install duckdb 

Run below to see version of duckdb
python -c "import duckdb; print(duckdb.__version__)"

##### pip install pandas 
Run below to see version of pandas
python -c "import pandas; print(pandas.__version__)"

##### pip install watchdog

##### python3 -m pip install watchdog

Exit Virtual Environment
deactivate 

Exit Bash
exit

### 7. Create a Python script for the DuckDB database setup:

Create scripts directory in ingest_data_from_file_with_python/
  mkdir scripts/
  cd scripts
  nano create_tables.py (the file is already available in scripts directory in main branch)

Press Ctrl + X, type Y, and hit Enter

Return to the project root:
cd ..

Create a data directory for the database file:
mkdir data/

### 8. Run the Python Script to Create the Database
  Activate the virtual environment:
  source venv/bin/activate  

  Run the scripts to create a DuckDB database named dynamic_state_ingestion_database.duckdb. 
  The below will create orders, order_states table, and also ingest data to these tables.
    1. python /Users/rasagna/ingest_data_from_file_with_python/scripts/create_tables.py --> new database (dynamic_state_ingestion_database.duckdb) will be created in /data directory 
    2. python /Users/rasagna/ingest_data_from_file_with_python/scripts/watch_directory_custom_script.py --> searches the /files/watch_directory_uploads once in every 30 seconds for new file
    3. python /Users/rasagna/ingest_data_from_file_with_python/scripts/watchdog_python_library.py --> works on observer pattern to keep an eye on every new upload in the /files/watchdog_uploads. This is unlike above which waits for next run to happen even if file was available
    
### 9. Connect to the Database Using a Client
To connect to the database using a client:
1. Download and install DBeaver.
2. Open DBeaver and create a new database connection.
3. Select DuckDB as the database type.
4. Browse to the database file located at: /Users/rasagna/ingest_data_from_file_with_python/data/dynamic_state_ingestion_database.duckdb
5. You can now view the newly created tables and its columns as defined in the create_tables.py script and also data in these tables

#### Note: 
When DBeaver (or any other client) is open and you try to run Python scripts that interact with a DuckDB database, you might encounter a conflict due to DuckDB's file-based locking mechanism. DuckDB ensures that only one process can write to the database at a time. If DBeaver (or another process) has the database open, other scripts cannot access or modify the database concurrently. 
To resolve this:
   1. Close or disconnect the database in DBeaver (or the other client) before running the script.
   2. Alternatively, you can run DBeaver in read-only mode to prevent write conflicts.

Congratulations!
You have successfully searched directories for new file uploads with two different approaches, processed the files as per the seach and process implementations respectively and persit data in order and order_states table of DuckDB database



 
