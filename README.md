# Data flow for testing remote-sensed data in crop simulations with the view to increase general availability and accessibility of inputs to crop models and boost scale-out

This weather data flow process was developed using Python. It involves three main steps: data preparation, data quality checking, and data presentation.

To run this code, you need to create a [Dropbox app](https://www.dropbox.com/developers/apps) under your Dropbox account. Follow the [Dropbox documentation](https://www.dropbox.com/developers/documentation/python#tutorial). Write the token on the key *tkn* into the config_file.json file. The Dropbox app directory must have the four directories specified in the *config_file.json file*: */raw_data/weather_raw*, */data_processed/weather_processed*, */data_temp/weather_temp*, */clean_data/weather_clean*. **Copy the weather data files into the *weather_raw* Dropbox directory**. The file names are created by default with the date and time when they are downloaded from the [REMAS website](https://www.siafeson.com/remas/index.php). Additionally, there is no identification for the station name because this information is stated in a column of each row. However, they are irrelevant for this process since the functions read all the comma-separated values (CSV) files that the directory conteins.
       

### Data preparation 

This proccess could be perform wheater using an Arflow DAG (*main.py*) or with the Python alternative scrip (*task_extract_transform.py*). 

- For running the *main.py* script, you may need to either [Running Airflow locally](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) or [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). Past the *main.py*,*functions.py*, and the *config_file.json* files in to the Airflow **dag** directory. This script requires the following Python libraries: *airflow*, *dropbox*,  *json*,  *datetime*, *os*, and *sys*.


- For running the *task_extract_transform.py* script instead of the Airflow option, this script has to be in the same directory as the *functions.py* and the *config_file.json* files.This script requires the following Python libraries: *dropbox*,  *json*,  *datetime*, *os*, and *sys*.
    
    
### Data quality checking methods and Data presentation 

The graphical data quality checking and the graphical analysis are performed using a dashboard application. Running them requires the following Python libraries: *dash*, *plotly*, *pandas *, and *json*. The *graphs_weather_cleanDF.py* executes the graphical data quality checking app while *graphs_weather_rawDF.py* runs the visual analysis. Once the app script is executed, the dashboard is visualized at http://127.0.0.1:8050/.
