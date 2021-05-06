# -*- coding: utf-8 -*-
'''
Created on April 2021
@author: luis vargas rojas (lvargasr@purdue.edu)
Spring 2021 ABE 65100 - Final project
'''
# App that builds graphs from weather data after the cleaning process

# Run this app with `python graphs_weather_cleanDF.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import dash_table
from datetime import date, timedelta
import datetime

import pandas as pd
import json

import functions


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


# read from json file the Token for accessing Dropbox files
path_file_json = '../config_file.json'
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

    # show the seasons menu
    html.Div(["Select season: ",
        dcc.Dropdown(
                  id='seasons_column',
                  options=[{'label': i, 'value': i} for i in seasons_list],
                  value=seasons_list[0])
    ],style={'width': '30%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # show the variables menu
    html.Div(["Select variable: ",
        dcc.Dropdown(
                  id='yaxis_column',
                  options=[{'label': i, 'value': i} for i in columns_list],
                  value=columns_list[1])
    ],style={'width': '30%', 'display': 'inline-block'}),

    html.Br(),

    # show the graph that can plot all the variables
    html.Div((
        dcc.Graph(
            id = 'variables-graphic'
        )
    ), style={'width': '70%', 'padding': '0px 20px 20px 20px'}),

    # show dates select botton
    html.Div(["Select the date range",
          dcc.DatePickerRange(
              id='date_range',
              min_date_allowed=df.index.min(),
              max_date_allowed=df.index.max(),
              start_date=datetime.datetime.strptime(df.index.min(), '%Y-%m-%d') + pd.DateOffset(days=120),
              end_date=datetime.datetime.strptime(df.index.min(), '%Y-%m-%d') + pd.DateOffset(days=130)
          ),
        ],style={'width': '30%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # Show the imput button for the label
    html.Div(['Write the text for label: ',
          dcc.Input(id='input1', type='text', value='Heading')
    ],style={'width': '18%', 'display': 'inline-block'}),

    html.Div(['Low Temperature: ',
          dcc.Input(id='input2', type='number', value=4)
    ],style={'width': '18%', 'display': 'inline-block'}),

    html.Div(['high Temperature: ',
          dcc.Input(id='input3', type='number', value=31)
    ],style={'width': '18%', 'display': 'inline-block'}),

    # Show the max and min temperature plot from of Block 910 station data
    html.Div([
        dcc.Graph(
            id = 'temperature-910'
        )
    ], style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # Show the histogram
    html.Div((
        dcc.Graph(
            id='histogram'
        )
    ), style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'}),

    # show the table with the missing data count
    html.Div((
         dash_table.DataTable(
             id='table_missing_val',
             columns=[{"name": i, "id": i}
                      for i in sum_missingDF.columns],
             export_format="csv",
             style_cell={
                 'textAlign': 'left',
                 'font_size': '20px'
             },
             style_data_conditional=[
                 {
                     'if': {'row_index': 'odd'},
                     'backgroundColor': 'rgb(248, 248, 248)'
                 }
             ],
             style_header={
                 'backgroundColor': 'rgb(230, 230, 230)',
                 'fontWeight': 'bold'
             }
         )
    ), style={'width': '70%', 'display': 'inline-block', 'padding': '0px 20px 0px 50px'})

])

# All variables graph
@app.callback(
    Output('variables-graphic', 'figure'),
    Input('yaxis_column', 'value'),
    Input('seasons_column', 'value'))

def update_graph(yaxis_column, seasons_column):
    # creates the graph that can plot all the variables
    #         yaxis_column: the name of the column name selected by to user
    #         seasons_column: season data group selected for the user

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
    Input('seasons_column', 'value'),
    [dash.dependencies.Input('date_range', 'start_date'),
        dash.dependencies.Input('date_range', 'end_date')],
    Input("input1", "value"),
    Input("input2", "value"),
    Input("input3", "value"))

def update_graph(seasons_column, start_date, end_date, input1, input2, input3):
    # creates the max and min temperature plot from of Block 910 station data
    #         seasons_column: season data group selected for the user

    # filter season
    seasonDF = df[df['Season'] == seasons_column]

    # filter station
    stationDF = seasonDF [seasonDF ['Station'] == 'BLOCK 910 (CIANO)']

    sdf = stationDF.iloc[:, [1, 2]]

    dff = pd.melt(sdf, ignore_index=False)

    # create the plot
    fig = px.line(dff, x=dff.index, y=dff.iloc[:,1], color=dff.iloc[:,0],
                  color_discrete_sequence=['firebrick', 'darkslategrey'],
                  labels={"color": "Group"}
                  )
    # add the vertical lines
    fig.add_vrect(x0=start_date, x1=end_date,
                  annotation_text=input1, annotation_position="top left",
                  fillcolor="green", opacity=0.25, line_width=0)

    # add the low values horizontal line
    fig.add_hline(y=input2, line_dash="dot",
                  annotation_text="Low critical value",
                  annotation_position="bottom right",
                  annotation_font_size=20
                  )

    # add the high values horizontal line
    fig.add_hline(y=input3, line_dash="dot",
                  annotation_text="High critical value",
                  annotation_position="top right",
                  annotation_font_size=20,
                  )

    # strip down the rest of the plot
    fig.update_layout(title='BLOCK 910 (CIANO)',
                      xaxis_title='Date',
                      yaxis_title='Temperature (°C)',
                      plot_bgcolor='lavender',
                      font_size=20,
                      font_color='#000000',
                      font_family='Old Standard TT'
                      )

    

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

# table with the missing data count
@app.callback(
    Output('table_missing_val', 'data'),
    Input('seasons_column', 'value'))

def tableFilter(seasons_column):
    # creates a table that contains the summary of the missing data values
    #            seasons_column: season data group selected for the user

    # filter season
    dff = df[df['Season'] == seasons_column]

    # get the missing values summary dataframe using the count_missing_values( )function
    sum_missingDF = functions.count_missing_values(dff)

    return sum_missingDF.to_dict('records')

if __name__ == '__main__':
    app.run_server(debug=True)