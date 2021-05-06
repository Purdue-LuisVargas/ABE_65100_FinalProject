from airflow import DAG
from airflow.operators.python import PythonOperator
import dropbox
from io import *
import json
import datetime
import os

# import the functions file
import sys
file = 'functions.py'
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
import functions

# DAG default args
default_args = {
    'owner': 'Luis Vargas',
    'start_date': datetime.datetime.now(),
    'schedule_interval': '@daily',
    #'retries': 1,
    'email': ['lvargasr@purdue.edu']
}

## Extract task
def extract_weather_data():
    # In this function all the steps of the extraction process are declared
    # Get the raw files from the Dropbox directory and transform them in to a dataframe, and move the files to processed files directory

    # read from json file the Token for accessing Dropbox files
    file = 'config_file.json'
    cwd = os.path.dirname(os.path.abspath(__file__))
    path_file_json = cwd + '/' + file
    ACCESS_TOKEN = json.load(open(path_file_json))['tkn']

    # init the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    # directory of the files to open
    weather_directory = json.load(open(path_file_json))['weather']['read']

    # directory where processed files will be copied
    weather_directory_copy = json.load(open(path_file_json))['weather']['copy']

    # get a list of the files in the Dropbox directory
    dbx_files = dbx.files_list_folder(weather_directory, recursive=True)

    # get the files and drop the directories
    files = [entry.path_lower for entry in dbx_files.entries if isinstance(entry, dropbox.files.FileMetadata) == True]

    # If the directory has files, they will be processed
    if len(files) > 0:

        # get the dataframe from the raw weather data
        weather_rawDF = functions.load_raw_weather_dataframe( dbx, ACCESS_TOKEN, weather_directory, weather_directory_copy )

        ### Save the new data frame into a the temp directory, if a file already exist merge it to the file, if not create a new file

        # directory where new dataframe will be copied
        weather_directory_temp = json.load(open(path_file_json))['weather']['temp']
        file_path = weather_directory_temp + '/temp_weather_df.csv'

        # get a list of the files in the Dropbox directory
        dbx_files = dbx.files_list_folder(weather_directory_temp, recursive=True)

        # get the name of file in the directory
        temp_filePath = [entry.path_lower for entry in dbx_files.entries if
                         isinstance(entry, dropbox.files.FileMetadata) == True]

        if len(temp_filePath) > 0:

            # get the file that already exists
            oldDF = functions.get_weater_temp_file_dropbox( ACCESS_TOKEN, temp_filePath[0] )

            # concatenate the new and the old file
            concatDF = functions.pd.concat([weather_rawDF, oldDF]).drop_duplicates()

            # save the dataframe
            functions.write_weater_temp_file_dropbox( ACCESS_TOKEN, concatDF, file_path )

        else:

            # save the dataframe
            functions.write_weater_temp_file_dropbox( ACCESS_TOKEN, weather_rawDF, file_path )

    else:

        print('There are not files to read in this directory')

# Transform task
def transform_weather_data():
    # In this function all the steps of the transformation process are declared

    # read from json file the Token for accessing Dropbox files
    file = 'config_file.json'
    cwd = os.path.dirname(os.path.abspath(__file__))
    path_file_json = cwd + '/' + file
    ACCESS_TOKEN = json.load(open(path_file_json))['tkn']

    # init the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    # Dropbox directory where the concatenated dataframe is stored
    weather_directory_temp = json.load(open(path_file_json))['weather']['temp']

    # get the dataframe from dropbox and transform the data into daily metrics
    dayDataDF = functions.metrics_weather(ACCESS_TOKEN, weather_directory_temp)


    # filter season
    seasons_column = 'Winter_2020-2021'
    seasonDF = dayDataDF[dayDataDF['Season'] == seasons_column]

    # filter each station
    station_1101 = 'BLOCK 1101'
    station_910 = 'BLOCK 910 (CIANO)'
    stationDF_1101 = seasonDF[seasonDF['Station'] == station_1101]
    stationDF_910 = seasonDF[seasonDF['Station'] == station_910]

    # fill missing values in dataframe from another dataframe
    stationDF_910 = stationDF_910.fillna(stationDF_1101)

    # directory where the concatenated dataframe is stored
    cleanDF_weather_directory = json.load(open(path_file_json))['weather']['clean']
    file_path = cleanDF_weather_directory + '/stationDF_910.csv'

    # get a list of the files in the Dropbox directory
    dbx_clean_files = dbx.files_list_folder(cleanDF_weather_directory , recursive=True)

    # get the name of file in the directory
    files_in_directory = [entry.path_lower for entry in dbx_clean_files .entries if
                     isinstance(entry, dropbox.files.FileMetadata) == True]

    if len(files_in_directory ) > 0:

        # get the file that already exists
        oldDF = functions.get_weater_temp_file_dropbox(ACCESS_TOKEN, files_in_directory[0])

        # concatenate the new and the old file
        concatDF = functions.pd.concat([stationDF_910, oldDF]).drop_duplicates()

        # save the dataframe
        functions.write_weater_temp_file_dropbox(ACCESS_TOKEN, stationDF_910 , file_path)

    else:

        # save the dataframe
        functions.write_weater_temp_file_dropbox(ACCESS_TOKEN, stationDF_910, file_path)


# define the DAG
dag = DAG('etl_weather', catchup=False, default_args=default_args)

# declare the tasks
Extract_weather_data_task = PythonOperator(
    task_id='Extract_weather_data',
    python_callable = extract_weather_data,
    dag = dag
)

transform_weather_data_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable = transform_weather_data,
    dag = dag
)

# Define the order in which the tasks will be executed
Extract_weather_data_task >> transform_weather_data_task