import sqlalchemy as db
import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
import ast
import logging

pd.options.mode.copy_on_write = True

logging.basicConfig(filename="clean_db.log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG,
                    force=True)

logger = logging.getLogger(__name__)


def cleanse_courses_df(courses_df: pd.DataFrame) -> pd.DataFrame:
    # adding no path option to career_path
    no_path = {'career_path_id': 0, 'career_path_name': 'no path', 'hours_to_complete': 0}
    courses_df.loc[len(courses_df)] = no_path

    return courses_df


def cleanse_jobs_df(jobs_df: pd.DataFrame) -> pd.DataFrame:
    # dropping duplicates
    jobs_df.drop_duplicates(inplace=True)

    return jobs_df


def cleanse_students_df(students_df: pd.DataFrame) -> pd.DataFrame:
    # creating age and age group columns, adjusting datatype for dob
    now = datetime.now()
    students_df['dob'] = pd.to_datetime(students_df['dob'])
    students_df['age'] = [(now.year - dob.year) for dob in students_df['dob']]
    students_df['age_group'] = np.int64((students_df['age'] / 10)) * 10

    # exploding contact info to email and street
    students_df['contact_info'] = students_df['contact_info'].apply(lambda x: ast.literal_eval(x))
    explode_contact_info = pd.json_normalize(students_df['contact_info'])
    students_df = pd.concat([students_df.drop('contact_info', axis=1), explode_contact_info], axis=1)

    split_address = students_df.mailing_address.str.split(',', expand=True)
    split_address.columns = ['street', 'city', 'state', 'zipcode']
    students_df = pd.concat([students_df.drop('mailing_address', axis=1), split_address], axis=1)

    # adjusting data_tyoes for job_id, num_course_taken, current_career_path_id, time_spent_hrs
    students_df['job_id'] = students_df['job_id'].astype('float')
    students_df['num_course_taken'] = students_df['num_course_taken'].astype('float')
    students_df['current_career_path_id'] = students_df['current_career_path_id'].astype('float')
    students_df['time_spent_hrs'] = students_df['time_spent_hrs'].astype('float')

    # dropping null values for columns
    students_df = students_df.dropna(subset=['num_course_taken'])
    students_df = students_df.dropna(subset=['job_id'])

    # filling in missing values for time_hrs_spent and current_career_path_id
    students_df['current_career_path_id'] = np.where(students_df['current_career_path_id'].isnull(), 0,
                                                     students_df['current_career_path_id'])
    students_df['time_spent_hrs'] = np.where(students_df['time_spent_hrs'].isnull(), 0, students_df['time_spent_hrs'])

    return students_df


# starting unit tests
def test_nulls(df):
    df_missing = df[df.isnull().any(axis=1)]
    len_missing = len(df_missing)

    try:
        assert len_missing == 0, "There are {} rows with missing data".format(len_missing)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("No null rows found")


def test_data_types(df, db_df):
    errors = 0
    for col in db_df:
        try:
            if df[col].dtypes != db_df[col].dtypes:
                errors += 1
        except NameError as ne:
            logger.exception(ne)

    if errors > 0:
        assert_message = "There are {} rows with missing data".format(errors)
        logger.error(assert_message)

        assert errors == 0, assert_message


def test_num_columns(df, db_df):
    try:
        assert len(df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("Number of columns are the same for both dataframes")


def test_joins_for_fan_out(students_df, merged_df):
    try:
        assert len(students_df) == len(merged_df)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("Number of students")


def main():
    logger.info("Start Log")

    # getting version from changelog
    with open('changelog.md', 'a+') as file:
        lines = file.readlines()
    if len(lines) == 0:
        next_version = 0
    else:
        # Version layout X.Y.Z
        next_version = int(lines[0].split('.')[2][0]) + 1

    # getting dataframes from dev database
    with db.create_engine('sqlite:///dev/cademycode.db').connect() as cademycode_conn:
        # importing data
        courses_query = db.text("select * from cademycode_courses")
        courses = cademycode_conn.execute(courses_query).fetchall()
        courses_df = pd.DataFrame(courses)

        jobs_query = db.text("select * from cademycode_student_jobs")
        jobs = cademycode_conn.execute(jobs_query).fetchall()
        jobs_df = pd.DataFrame(jobs)

        students_query = db.text("select * from cademycode_students")
        students = cademycode_conn.execute(students_query).fetchall()
        students_df = pd.DataFrame(students)

    # checking prod to see if there are new students in the dev database which need to be cleaned
    try:
        with sqlite3.connect('../prod/cademycode_clean.db') as prod_conn:
            clean_db = pd.read_sql_query('SELECT * FROM student_info', prod_conn)

        new_students = students_df[~np.isin(students_df.uuid.unique(), clean_db.uuid.unique())]
    except:
        new_students = students_df
        clean_db = []

    # cleansing students df
    clean_new_students = cleanse_students_df(new_students)

    if len(clean_new_students) > 0:
        # cleansing courses and jobs dfs
        clean_courses = cleanse_courses_df(courses_df)
        clean_jobs = cleanse_jobs_df(jobs_df)

        # merging dataframes
        new_merged_df = clean_new_students.merge(clean_courses, left_on='current_career_path_id',
                                                   right_on='career_path_id', how='left')
        new_merged_df = new_merged_df.merge(clean_jobs, on='job_id', how='left')

        # calling unit tests
        if len(clean_db) > 0:
            test_data_types(new_merged_df, clean_db)
            test_num_columns(new_merged_df, clean_db)
            test_joins_for_fan_out(new_merged_df, clean_db)
        test_nulls(new_merged_df)

        # uploading clean merged dataframe to dev clean database
        with sqlite3.connect('./dev/cademycode_clean.db') as clean_conn:
            new_merged_df.to_sql('student_info', clean_conn, if_exists='append', index=False)
            clean_db = pd.read_sql_query("SELECT * FROM student_info", clean_conn)

        # writing clean df to file
        clean_db.to_csv('./dev/student_info_cleansed.csv', index=False)

        # creating lines to be written to changelog
        new_lines = [
            '## 0.0.' + str(next_version) + '\n' +
            '### Added\n' +
            ' ' + str(len(new_merged_df)) + ' more rows of raw data added to student_info \n' +
            '\n'
        ]
        write_lines = ''.join(new_lines + lines)

        # writing lines to changelog
        with open('changelog.md', 'w') as file:
            for line in write_lines:
                file.write(line)
    else:
        print("no new data")
        logger.info("no_new_data")

    logger.info("End Log")


if __name__ == '__main__':
    main()
