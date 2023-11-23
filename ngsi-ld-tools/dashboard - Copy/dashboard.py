import datetime
import json
import logging
import os
import random
import sys
import time
from datetime import timezone
from threading import Thread

import dash_leaflet as dl
import dash_leaflet.express as dlx
import requests
import schedule
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
from dash_extensions.javascript import assign
import dash_leaflet as dl
import dash_leaflet.express as dlx
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import time
import base64
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
#from mtk_common import utils

LOGGER = logging.getLogger("ngsildmap")
LOGGER.setLevel(10)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
sh.setLevel(10)
LOGGER.addHandler(sh)
LOGGER.info("NGSI-LD Map starting...")


ROUNDINGVALUE = 0



  
def getEntityTypes():
  result = []
  types = requests.get('http://localhost:9090/ngsi-ld/v1/types').json()['typeList']
  for entityType in types:
    result.append({"label": html.Div([entityType]),"value": entityType,"internal": True})
  return result;


app = Dash(external_stylesheets=[dbc.themes.DARKLY])
#initialSetup(app)
image_filename = 'assets/ScorpioLogo.png' # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())
app.layout = html.Div([
    html.Div([
      html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()), style={'display': 'inline-block', 'height': '80px'}),
      html.H2('Scorpio Dashboard', style={'display': 'inline-block', 'padding-left': 2, 'vertical-align': 'middle', 'height': '50'})
    ], id='header_div', style={'text-align': 'center', 'padding': 5}),
    html.Div([
      html.Div([
        dbc.Input(id="entity-type-input", placeholder="Add an additional Entity Type...", type="text"),
        dbc.Button(id="entity-type-add-button","Add Type", color="primary"),
        html.Br(),
        dcc.Checklist(getEntityTypes(), id='entity-type-selection')
      ], id='entity-types-list')
    ], id='type-selection-container', style={'padding': 5})
])

@app.callback(
  Output('entity-type-input', 'value')
  Output('entity-type-selection', 'children'),
  Output('entity-type-selection', 'value'),
  Input('entity-type-add-button', 'n_clicks'),
  State('entity-type-input', 'value'),
  State('entity-type-selection', 'children'),
  State('entity-type-selection', 'value'),)
def addEntityType(n, newType, entityTypes, selectedEntityTypes):
  if n:
    entityTypes.append({"label": html.Div([newType]),"value": newType,"internal": False})
    selectedEntityTypes.append(newType)
  return '', entityTypes, selectedEntityTypes


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050)
