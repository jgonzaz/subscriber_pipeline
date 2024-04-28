# Subscriber Pipeline Write Up

## Data Cleansing Decisions
### Students table
After looking at the students table there were several columns with missing data. I used a series of 
visualizations and EDA python functions to deem whether the data was Missing at Random. Using this technique 
I was able to deem job_id and num_course_taken columns were Missing at Random. Since rows with missing values 
represented a small portion of the overall data, I went ahead and removed the rows with missing data. 

Two columns did not fall in line with the standards of Missing at Random data and instead were deemed to be 
missing structurally. I filled in the missing values and continued with my prepping of the students data.

The contact_info column stored contact information for the student in a dictionary which I normalized so that the 
information was more accessible. I also created age and age_group columns based of the dob column so that future 
analysts would have an easier time looking at the demographics of the student population.

There were a series of columns with misaligned datatypes, so I also adjusted the datatypes to ones more suitable for 
what the data represents.

### Courses table 
Added a value in accordance with filling in missing value for current_career_path_id column in school dataframe.

### Jobs table 
After assessing the data, I noticed there were a few rows that were duplicated. I removed duplicate rows so that 
future joins would not cause fan out.

## Automation via Bash Script
The bash script allows for semi-automated cleansing and conforming of the underlying datasets through calling the 
main function in the script_ingest.py file. The python script includes the following unit tests to ensure everything 
runs as smoothly as possible:
- test nulls
- test data types
- test number of columns
- test joins for fanout

If the unit tests fail then the script will end and the errors will be logged in clean_db.log file. If the tests pass 
then bash script will compare the versions present in changelogs of the dev and prod version to see if there are new 
changes that need to be added to the prod folder. If the versions are different then the bash script will copy files 
from the dev folder to the prod folder but if they are the same no copy will be performed and the user will be signaled 
that no changes are detected.


