## Data flow for testing remote-sensed data in crop simulations with the view to increase general availability and accessibility of inputs to crop models and boost scale-out

This weather data flow process was developed using Python. It involves three main steps: data preparation, data quality checking, and data presentation.

### Before start 

To run this code, you need to create a [Dropbox app](https://www.dropbox.com/developers/apps) under your Dropbox account. Follow the [Dropbox documentation](https://www.dropbox.com/developers/documentation/python#tutorial). Write the token on the key *tkn* into the ***config_file.json*** file. The Dropbox app directory must have the four directories specified in the *config_file.json file*:
- */raw_data/weather_raw*, 
- */data_processed/weather_processed*
- */data_temp/weather_temp*
- */clean_data/weather_clean* 

Copy the weather data files into the *weather_raw* Dropbox directory. 
       

### Data preparation 

This proccess could be perform wheater using an Arflow DAG (*main.py*) or with the Python alternative scrip (*task_extract_transform.py*). 

- For running the *main.py* script, you may need to either [Running Airflow locally](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) or [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). Past the *main.py*,*functions.py*, and the *config_file.json* files in to the Airflow **dag** directory. This script requires the following Python libraries: *airflow*, *dropbox*,  *json*,  *datetime*, *os*, and *sys*.


- For running the *task_extract_transform.py* script instead of the Airflow option, this script has to be in the same directory as the *functions.py* and the *config_file.json* files.This script requires the following Python libraries: *dropbox*,  *json*,  *datetime*, *os*, and *sys*.
    
    
### Data quality checking methods and Data presentation 

The graphical data quality checking and the graphical analysis are performed using a dashboard application. Running them requires the following Python libraries: *dash*, *plotly*, *pandas *, and *json*. 

- The *graphs_weather_rawDF.py* executes the graphical data quality checking app

- The *graphs_weather_cleanDF.py* runs the visual data analysis once the data are clean 

After running the app script, the dashboard is visualized at http://127.0.0.1:8050/. These scripts requiere the *functions.py* and the *config_file.json* files.

## Running weather data flow process (summary steps)

1. Open a Dropbox app and create the directories that are specified in the before start instructions


2. Upload the data files from the **./data** directory into your Dropbox app directory **/raw_data/weather_raw**


3. Replace the token contained into **config_file.json** with the Dropbox token of your app

        "tkn": "gfsVdgP0tQYAAAAAAAAAAQ9ks-Wi-fb8PzJ4pyxxbVRavzE-97fsG7_1Ppdls-vZ"
    
4. Run the **etl_weather** if you have chosen the Airflow option from Data preparation directions, or run the **task_extract_transform.py** script if you have preferred the alternative option. 


5. Run the file **graphs_weather_rawDF.py** and visit http://127.0.0.1:8050/ at your web browser to explore the raw data in the dashboard with the interactive graphs. Close the app before running the next step.


6. Run the file **graphs_weather_cleanDF.py** and visit http://127.0.0.1:8050/ at your web browser to explore the dashboard with the interactive graphs for data analysis.