# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_table

import pandas as pd
import json

import sys
sys.path.insert(0,'/home/luis/airflow/dags/remote_sensing/etl_weather/')
import functions


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


# read from json file the Token for accessing Dropbox files
path_file_json = '/home/luis/airflow/dags/remote_sensing/etl_weather/config_file.json'
ACCESS_TOKEN = json.load(open(path_file_json))['tkn']

# Dropbox directory where the concatenated dataframe is stored
weather_directory_temp = json.load(open(path_file_json))['weather']['clean']
fileName = weather_directory_temp + '/stationDF_910.csv'

# get the dataframe from dropbox and transform the data into daily metrics
df = functions.get_weater_temp_file_dropbox(ACCESS_TOKEN, fileName)

# get the missing values summary dataframe
sum_missingDF = functions.count_missing_values(df)

# get the list of columns for the selection menu
columns_list = df.columns.tolist()[1:len(df.columns.tolist()) - 1 ]

# get the list of seasons for the selection menu
seasons_list = [item for item in df['Season'].unique().tolist() if item != 'Spring']


# html layout
app.layout = html.Div([
    html.H4('Spring 2021 ABE 65100'),
    html.H6('Final project'),

    html.Div((
        html.H3('Graphical exploratory analysis')
    ), style={'width': '49%', 'padding': '0px 40px 0px 20px'}),

    # menu seasons
    html.Div(["Select season: ",
        dcc.Dropdown(
                  id='seasons_column',
                  options=[{'label': i, 'value': i} for i in seasons_list],
                  value=seasons_list[0])
    ],style={'width': '30%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # menu variables
    html.Div(["Select variable: ",
        dcc.Dropdown(
                  id='yaxis_column',
                  options=[{'label': i, 'value': i} for i in columns_list],
                  value=columns_list[1])
    ],style={'width': '30%', 'display': 'inline-block'}),

    html.Br(),

    # All variables graph
    html.Div((
        dcc.Graph(
            id = 'variables-graphic'
        )
    ), style={'width': '70%', 'padding': '0px 20px 20px 20px'}),

    # Temperature max and min Block 910
    html.Div([
        dcc.Graph(
            id = 'temperature-910'
        )
    ], style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # Histogram
    html.Div((
        dcc.Graph(
            id='histogram'
        )
    ), style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # Table
    html.Div((
        dash_table.DataTable(
            id='table',
            columns=[{"name": i, "id": i}
                     for i in sum_missingDF.columns],
            data=sum_missingDF.to_dict('records'),
            style_cell=dict(textAlign='left'),
            style_header=dict(backgroundColor="paleturquoise"),
            style_data=dict(backgroundColor="lavender")
        )
    ), style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'})

])

# All variables graph
@app.callback(
    Output('variables-graphic', 'figure'),
    Input('yaxis_column', 'value'),
    Input('seasons_column', 'value'))

def update_graph(yaxis_column, seasons_column):
    # create the graph

    # filter season
    dff=df[df['Season']==seasons_column]
    fig = px.line(dff, x=dff.index, y= yaxis_column
                  )

    # strip down the rest of the plot
    fig.update_layout(title='BLOCK 910 (CIANO)', xaxis_title='Date',
                      yaxis_title= yaxis_column,
                      plot_bgcolor='lavender',
                      font_size=20,
                      font_color='#000000',
                      font_family='Old Standard TT')
    return fig

# Min and max temperature graph BLOCK 910 (CIANO)
@app.callback(
    Output('temperature-910', 'figure'),
    Input('seasons_column', 'value'))

def update_graph(seasons_column):
    # create the graph

    # filter season
    seasonDF = df[df['Season'] == seasons_column]

    # filter station
    stationDF = seasonDF [seasonDF ['Station'] == 'BLOCK 910 (CIANO)']

    sdf = stationDF.iloc[:, [1, 2]]

    dff = pd.melt(sdf, ignore_index=False)

    fig = px.line(dff, x=dff.index, y=dff.iloc[:,1], color=dff.iloc[:,0],
                  color_discrete_sequence=['firebrick', 'darkslategrey'],
                  labels={"color": "Group"}
                  )

    # strip down the rest of the plot
    fig.update_layout(title='BLOCK 910 (CIANO)',
                      xaxis_title='Date',
                      yaxis_title='Temperature (°C)',
                      plot_bgcolor='lavender',
                      font_size=20,
                      font_color='#000000',
                      font_family='Old Standard TT')

    return fig

# Histogram
@app.callback(
    Output('histogram', 'figure'),
    Input('seasons_column', 'value'))

def update_graph(seasons_column):
    # create the graph

    # filter season
    dff = df[df['Season'] == seasons_column]
    fig = px.histogram(dff, x=dff.index, y=dff['Total rainfall (mm)'],
                       histfunc='sum'
                  )

    # strip down the rest of the plot
    fig.update_layout(title='BLOCK 910 (CIANO)',
                      yaxis_title='Total rainfall (mm)',
                      plot_bgcolor='lavender',
                      font_size=20,
                      font_color='#000000',
                      font_family='Old Standard TT')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)