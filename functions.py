import pandas as pd
import dropbox
from io import *
import logging
import numpy as np

#### functions
def get_raw_weather_file_dropbox(ACCESS_TOKEN, fileName):
    ''' download the data file from Dropbox. https://villoro.com/post/dropbox_python
            ACCESS_TOKEN: Dropbox token
            fileName: path and name of the file to download'''

    # start the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    try:

        # download the file in an io.BytesIO object
        _, res = dbx.files_download(fileName)

        # Transform from bits to data frame the file downloaded from Dropbox
        with BytesIO(res.content) as stream:

            # option parse_dates=[['Fecha', 'hora minuto']] extract these two columns into one and reads it as date field
            dataFrame = pd.read_csv(stream, index_col=False, encoding='latin1', parse_dates=[[1, 2]])
            # rename the parsed date column
            dataFrame.columns.values[0] = 'Date'
            dataFrame = dataFrame.set_index('Date')

        return dataFrame

    except Exception as e:

        logging.error('Exception occurred', exc_info=True)

def load_raw_weather_dataframe(dbx, ACCESS_TOKEN, directoryRead, directoryCopy):
    '''Load files from a dropbox directory with the same structure and merge them into a single data frame.
    The files processed will be moved to another dropbox directory.
        dbx: the dropbox object
        directoryRead: dropbox directory that conteins the files to open
        directoryCopy: dropbox directory where processed files will be copied'''

    # get a list of the files in the Dropbox directory
    dbx_files = dbx.files_list_folder(directoryRead, recursive=True)

    # get the files and drop the directories
    files = [entry.path_lower for entry in dbx_files.entries if isinstance(entry, dropbox.files.FileMetadata) == True]

    # If the directory has files, they will be processed
    if len(files) > 0:

        # create an empty data frame object to concatenate the each new data frame
        merged_df = pd.DataFrame()

        print(str(len(files)) + ' files to process' + '\n')

        # read and process each of the files in the list
        for fileName in files:
            print("Processing: " + fileName)

            # Get the fileName station weater data
            new_df = get_raw_weather_file_dropbox(ACCESS_TOKEN, fileName)
            print('\tFile shape: ' + str(new_df.shape) + '\n')

            # add the data frame to the
            merged_df = merged_df.append(new_df)

            # transform the dataframe index from date to datetime
            merged_df.index = pd.to_datetime(merged_df.index)

            # move processed file
            dbx.files_move(fileName, directoryCopy + '/' + fileName.split('/')[-1], autorename=False)

        print('Final data frame shape: ' + str(merged_df.shape) + '\n')

        print("Done!")

        return merged_df

    else:

        print('There are not files to read in this directory')


def get_weater_temp_file_dropbox(ACCESS_TOKEN, fileName):
    ''' download the data file from Dropbox. https://villoro.com/post/dropbox_python
            ACCESS_TOKEN: Dropbox token
            fileName: path and name of the file to download'''

    # start the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    try:

        # download the file in an io.BytesIO object
        _, res = dbx.files_download(fileName)

        # Transform from bits to data frame the file downloaded from Dropbox
        with BytesIO(res.content) as stream:

            # option parse_dates=[['Fecha', 'hora minuto']] extract these two columns into one and reads it as date field
            dataFrame = pd.read_csv(stream, index_col='Date', encoding='latin1')
            # rename the parsed date column
            # dataFrame.columns.values[0] = 'Date'
            # dataFrame = dataFrame.set_index('Date')

        return dataFrame

    except Exception as e:

        logging.error('Exception occurred', exc_info=True)

def write_weater_temp_file_dropbox(ACCESS_TOKEN, dataframe, filePath):
    '''function that saves a dataframe into a csv file on a defined directory of Dropbox
            dataframe: a dataframe to save
            filePath: directory and name of file to save
            ACCESS_TOKEN: Dropbox token'''

    # start the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    # transform the dataframe inte a csv object
    df_string = dataframe.to_csv()

    # transform the csv object into bytes
    db_bytes = bytes(df_string, 'latin-1')

    try:

        # save the bytes object into a csv file
        dbx.files_upload(f=db_bytes, path=filePath, mode=dropbox.files.WriteMode.overwrite)

    except Exception as e:

        logging.error('Exception occurred', exc_info=True)

def metrics_weather(ACCESS_TOKEN, weather_directory_temp):
    '''Transform the raw hourly observations weather data frame into a resample data frame with daily calculations
        weather_directory_temp: Dropbox directory where the file is stored
        ACCESS_TOKEN: Dropbox token'''

    # init the dropbox object
    dbx = dropbox.Dropbox(ACCESS_TOKEN)

    # get a list of the files in the Dropbox directory
    dbx_files = dbx.files_list_folder(weather_directory_temp, recursive=True)

    # get only the files avoid directories
    temp_filePath = [entry.path_lower for entry in dbx_files.entries if
                     isinstance(entry, dropbox.files.FileMetadata) == True]

    tempDF = get_weater_temp_file_dropbox(ACCESS_TOKEN, temp_filePath[0])

    # transform the dataframe index from date to datetime
    tempDF.index = pd.to_datetime(tempDF.index)

    # drop rows from other stations
    stations = ['BLOCK 910 (CIANO)', 'BLOCK 1101']
    DataDF = tempDF[tempDF['Estación'].isin(stations)]

    # select columns to use
    DataDF = DataDF[['Estación', 'Temperatura máxima', 'Temperatura mínima', 'Humedad relativa',
                     'Precipitación', 'Radiación solar', 'Velocidad del viento', 'Evapotranspiración']]

    # create a dict to rename columns from Spanish to English
    columnsDict = {'Estación': 'Station', 'Temperatura máxima': 'Maximun temperature (°C)',
                   'Temperatura mínima': 'Minimum  temperature (°C)', 'Humedad relativa': 'Humidity (%)',
                   'Precipitación': 'Total rainfall (mm)', 'Radiación solar': 'Solar radiation (W/m2)',
                   'Velocidad del viento': 'Wind speed (km/h)', 'Evapotranspiración': 'Evapotranspiration (mm)'}

    # rename the columns using the dictionary
    DataDF.rename(columnsDict, axis='columns', inplace=True)

    # create a dictionary whit the calculations for each column
    fnc = {'Maximun temperature (°C)': 'max',
           'Minimum  temperature (°C)': 'min',
           'Humidity (%)': 'mean',
           'Total rainfall (mm)': 'sum',
           'Solar radiation (W/m2)': 'mean',
           'Wind speed (km/h)': 'mean',
           'Evapotranspiration (mm)': 'sum'}

    # resample by the daily values acording each calculation
    dayDataDF = DataDF.groupby('Station').resample('D').agg(fnc)

    # reset and rename the index of the new data frame
    dayDataDF = dayDataDF.reset_index()
    dayDataDF = dayDataDF.set_index('Date')

    # The agregate sum function does not drop NaN values.
    # This instuction fill with NaN the same rows where the "Maximun temperature (°C)" is NaN
    dayDataDF['Evapotranspiration (mm)'] = dayDataDF['Evapotranspiration (mm)'].mask(
        dayDataDF['Maximun temperature (°C)'].isnull(), np.nan)
    dayDataDF['Total rainfall (mm)'] = dayDataDF['Total rainfall (mm)'].mask(
        dayDataDF['Maximun temperature (°C)'].isnull(), np.nan)

    # create groups by growth season
    dayDataDF['Season'] = np.where((dayDataDF.index.month == 11) | (dayDataDF.index.month == 12),
                                   'Winter_' + dayDataDF.index.year.astype(str) + '-' + (
                                           dayDataDF.index.year + 1).astype(str),
                                   'Winter_' + (dayDataDF.index.year - 1).astype(
                                       str) + '-' + dayDataDF.index.year.astype(str))

    dayDataDF['Season'] = np.where(
        (dayDataDF.index.month == 5) | (dayDataDF.index.month == 6) | (dayDataDF.index.month == 7) | (
                dayDataDF.index.month == 8)
        | (dayDataDF.index.month == 9) | (dayDataDF.index.month == 10), 'Spring', dayDataDF['Season'])

    return dayDataDF


def count_missing_values(dataFrame):
    '''Function that returns a summary data frame of missing values
            dataFrame: a data frame to evaluate '''

    # sumarize the missing values
    missing_values = dataFrame.isnull().sum().reset_index()

    # rename columns
    missing_values.columns = ['Column', 'Missing values']

    # get the total rows of the data frame
    missing_values['Total rows'] = dataFrame.shape[0]

    # create a new column with the missing values ratio
    missing_values['Missing values (%)'] = round(
        (missing_values['Missing values'] / missing_values['Total rows'] * 100), 2)

    return missing_values