# Subscriber Pipeline

## Steps to Run Process
1. Run automate_pipeline.sh and follow the prompts 
2. If prompted the script will run script_ingest_data.py. This script handles the cleaning and processing of 3 table 
to one source of truth
3. If unit tests fail, errors will be written to clean_db.log and process will be stopped
4. Bash script will check for updates via the changelog files in dev and prod
5. If changes are present and user prompts to move the files. 
cademycode_clean.db, student_info_cleansed.csv and changelog.log will be copied to prod


## Project Structure
- automate_pipeline.sh: bash script that handles the cleaning and ingestion of clean data to prod as well as movement of 
files from dev folder to proc
- changelog.md: Both dev and prod folders contain a changelog.md file with information pertaining to updates with each run of the process.
Updates are seperated used an X.Y.Z version.


### dev folder:
- cademycode.db: source database
- cademycode_updated.db: updated db used for testing pipeline 
- clean_db.log: file holds log info including errors from the running of script_ingest_data.py
- script_ingest_data.py: python file housing the scripting for cleaning and process source data.

### prod folder:
- cademycode_clean.db: end result of running cleaning in process. source of truth for data consumers
- student_info_cleansed.csv: clean df in csv format copied from dev folder by running bash script

### Other:
- notebook-data_cleansing.ipynb: jupyter notebook housing original run through of data cleaning and ingestion
- write_up.md: explanation of methodologies and thought process.
