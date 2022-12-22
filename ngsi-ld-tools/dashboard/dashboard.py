import datetime
import json
import logging
import os
import random
import sys
import time
import base64
from datetime import timezone
import requests
from dash import Dash, dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
import urllib.parse

LOGGER = logging.getLogger("ngsildmap")
LOGGER.setLevel(10)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
sh.setLevel(10)
LOGGER.addHandler(sh)
LOGGER.info("Scorpio Dashboard starting...")


ROUNDINGVALUE = int(os.getenv('SCORPIO_DASHBOARD_ROUNDING_VALUE', '0'))

PRECONFIGURED_QUERIES = os.getenv('SCORPIO_DASHBOARD_QUERIES', None)
if PRECONFIGURED_QUERIES:
  PRECONFIGURED_QUERIES = json.loads(PRECONFIGURED_QUERIES)
else:
  PRECONFIGURED_QUERIES = []
POLL_TIME = os.getenv('SCORPIO_DASHBOARD_POLL_TIME', 5000)

if PRECONFIGURED_QUERIES == []:
  hiddenConfig = ''
  timerDisabled = True
  showHosts = True
  showGraphsHidden = 'hidden'
else:
  hiddenConfig = 'hidden'
  timerDisabled = False
  showHosts = False
  showGraphsHidden = ''

app = Dash(external_stylesheets=[dbc.themes.DARKLY])
#initialSetup(app)
image_filename = 'assets/ScorpioLogo.png' # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())
app.layout = html.Div([
    html.Div([
      html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()), style={'display': 'inline-block', 'height': '80px'}),
      html.H2('Scorpio Dashboard', style={'display': 'inline-block', 'padding-left': 2, 'vertical-align': 'middle', 'height': '50'})
    ], id='header_div', style={'text-align': 'center', 'padding': 5}),
    html.Button('⮟Hosts⮟', id='show-hide-hosts-button', n_clicks=0, style={'width': '100%', 'height': '14px', 'backgroundColor': 'rgb(34,34,34)', 'color': 'white', 'border': '0px', 'margin': 'auto', 'font-size': '10px'}, hidden=hiddenConfig),
    dbc.Collapse([  
      html.Div([
        html.Div([
          html.Div([
            html.Div([
              html.Label('NGSI-LD Host', style={ 'width': 150, 'display': 'inline-block'}),
              dcc.Input(
                id='host-input',
                placeholder='Enter the url of a NGSI-LD broker',
                value='http://localhost:9090',
                style={ 'display': 'inline-block', 'padding': 1, 'backgroundColor': 'darkgrey', 'color': 'white', 'border': '0px'}
              )
            ], style={'padding': 2}),
            html.Div([
              html.Label('@context', style={ 'width': 150, 'display': 'inline-block'}),
              dcc.Input(
                id='atcontext-input',
                value='https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld',
                style={ 'display': 'inline-block','padding': 1, 'backgroundColor': 'darkgrey', 'color': 'white', 'border': '0px'}
              )
            ], style={'padding': 2}),
            html.Div([
              html.Label('IDSA connector', style={ 'width': 150, 'display': 'inline-block'}),
              dcc.Checklist(id='idsa-check', options = [{'label': '', 'value': 'isIDSA'}], style={ 'display': 'inline-block', 'color': 'darkgrey', 'border': '0px'})
            ]),
          ], style={'display': 'inline-block'}),
          html.Div([
            html.Button('Add Host', id='add-host-button', n_clicks=0, style={'backgroundColor': 'black', 'color': 'white', 'border': '1px solid white'})
          ], style={'display': 'inline-block', 'vertical-align': 'top', 'padding': 2}),
        ], style={'width': '25vw', 'display': 'inline-block'}),
        html.Div([
          dash_table.DataTable(
            id='hosts-table',
            columns=[
              {
              'name': 'Host',
              'id': 'url'},
              {
              'name': '@Context',
              'id': 'atContext'},
              {
              'name': 'IDSA Connector',
              'id': 'isIDSA'}
            ],
            data=[],
            editable=True,
            row_deletable=True,
            page_size=4,
            style_header={
              'backgroundColor': 'grey',
              'border': '1px solid black'
            },
            style_data={
              'backgroundColor': 'darkgrey',
              'border': '1px solid black'
            })
        ], style={'width': '70vw', 'display': 'inline-block', 'vertical-align': 'top', 'float': 'right'})
      ], style={'padding': 5})
    ], id='hosts-collabsable', is_open=showHosts),
    html.Div([
      html.Button('⮟Queries⮟', id='show-hide-queries-button', n_clicks=0, style={'width': '100%', 'height': '14px', 'backgroundColor': 'rgb(34,34,34)', 'color': 'white', 'border': '0px', 'margin': 'auto', 'font-size': '10px'}, hidden=hiddenConfig),
      dbc.Collapse([
        html.Div([
          html.Div([
            dcc.Dropdown(id="type2attrs-dropdown",placeholder='Select an attribute please',multi=False, searchable=False),
            html.Div([
              html.Label('Query name:', style={'width': 130, 'display': 'inline-block', 'backgroundColor': 'grey', 'padding': 1}),
              dcc.Input(
                id='query-name',
                placeholder='Enter a query name',
                style={ 'display': 'inline-block', 'padding': 1, 'backgroundColor': 'darkgrey', 'color': 'white', 'border': '0px'}
              )
            ], style={'padding': 1}),
            html.Div([
              html.Label('NGSI-LD query:', style={'width': 130, 'display': 'inline-block', 'backgroundColor': 'grey', 'padding': 1}),
              dcc.Input(
                  id='q-input',
                  placeholder='Enter a q query',
                  style={ 'display': 'inline-block', 'padding': 1, 'backgroundColor': 'darkgrey', 'color': 'white', 'border': '0px'}
                )
            ], style={'padding': 1}),
            html.Div([
              html.Label('Geo query:', style={'width': 130, 'display': 'inline-block', 'backgroundColor': 'grey', 'padding': 1}),
              dcc.Input(
                id='geoq-input',
                placeholder='Enter a geo query',
                style={ 'display': 'inline-block', 'padding': 1, 'backgroundColor': 'darkgrey', 'color': 'white', 'border': '0px'}
              )
            ], style={'padding': 1})
          ], style={'width': '100%'}),
          html.Div([
            html.Button('Add Query', id='add-query-button', n_clicks=0, style={'backgroundColor': 'black', 'color': 'white', 'border': '1px solid white'})
          ])
        ], style={'width': '28vw', 'display': 'inline-block'}),
        html.Div([
          dash_table.DataTable(
            id='query-table',
            columns=[
              {
                'name': 'Query Name',
                'id': 'qId'},
              {
                'name': 'Host',
                'id': 'url'},
              {
                'name': '@Context',
                'id': 'atContext'},
              {
                'name': 'IDSA Connector',
                'id': 'isIDSA'},
              {
                'name': 'Type Attrib Combination',
                'id': 'type2Attrib'},
              {
                'name': 'Q',
                'id': 'q'},
              {
                'name': 'geoQ',
                'id': 'geoQ'}
            ],
            data=PRECONFIGURED_QUERIES,
            editable=False,
            row_deletable=True,
            page_size=5,
            style_header={
              'backgroundColor': 'grey',
              'border': '1px solid black'
            },
            style_data={
              'backgroundColor': 'darkgrey',
              'border': '1px solid black'
            })
        ], style={'width': '70vw', 'display': 'inline-block', 'vertical-align': 'top', 'float': 'right'})
      ], id='queries-collabsable', is_open=timerDisabled)
    ], id='type2attrs-dropdown_container', hidden='hidden', style={'padding': 5}),
    html.Div([
      html.Div([
        dash_table.DataTable(
          id='entities-table',
          columns=[
            {
              'name': 'Entity ID',
              'id': 'entityId',
              'selectable': False},
            {
              'name': 'Entity Type',
              'id': 'entityType',
              'selectable': False},          {
              'name': 'Selected Attrib',
              'id': 'attrib',
              'selectable': False},
            {
              'name': 'Attrib Value',
              'id': 'attrib_value',
              'selectable': False},
          ],
          data=[],
          row_selectable="multi",
          column_selectable=None,
          editable=False,
          page_size=11,
          style_header={
            'backgroundColor': 'grey',
            'border': '1px solid black'
          },
          style_data={
            'backgroundColor': 'darkgrey',
            'border': '1px solid black'
          }
        )
      ], id='entities-table_container', style={'width': '49vw', 'display': 'inline-block', 'vertical-align': 'top', 'padding-right': 1, 'height': '49vh'}),
      html.Div([
        dcc.Graph(
          id='entities-map',
          config={
            'displayModeBar': False
          }
        )
      ], id='entities_map_container', style={'width': '49vw', 'display': 'inline-block', 'vertical-align': 'top', 'float': 'right', 'height': '49vh'})
    ], id='entities_view_container', hidden=showGraphsHidden, style={'height': '49vh'}),
    html.Div([
      dcc.Graph(
        id='history-line-graph',
        figure=px.line(title='History Graph', template='plotly_dark')
      )
    ], id='history-line-graph-container', hidden='hidden'),
    html.Div([
      dcc.Graph(
        id='value_distribution_graph',
        figure=px.bar(title='Value Distribution', template='plotly_dark'),
        style={'display': 'inline-block', 'width': '78vw'}
      ),
      dcc.Graph(
        id='min_max_avg_graph',
        figure=px.bar(title='Minimum, Maximum, Average', template='plotly_dark'),
        style={'display': 'inline-block', 'width': '20vw', 'float': 'right'}
      )
    ], id='live_graph_container', hidden=showGraphsHidden),
    dcc.Interval(
      id="interval-component",
      interval=POLL_TIME,  # in milliseconds
      n_intervals=int(not timerDisabled),
      disabled=timerDisabled
    )
])

def temporalGet(url, headers, isIDSA, entityId, attrib):
  if isIDSA:
    print('do nothing')
  else:
    return requests.get(url + '/ngsi-ld/v1/temporal/entities/' + entityId + '?attrs=' + attrib + '&options=sysAttrs'
      ,headers=headers,
      ).json()
def getTypeEndpoints(host, atContext, isIDSA):
  result = []
  if isIDSA:
    print('do nothing')
  else:
    headers={
      "Link": "<"
       + atContext
       + '>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"',
      "Accept": "application/json",
    }
    types = requests.get(host + '/ngsi-ld/v1/types'
      ,headers=headers,
      ).json()['typeList']
    for entityType in types:
      result.append({'headers': headers, 'url': host + '/ngsi-ld/v1/types/' + urllib.parse.quote(entityType, safe=''), 'baseUrl': host, 'type': entityType, 'isIDSA': False, 'atContext': atContext})
  return result
def getAttribs(typeEndpoints, isIDSA):
  type2Attribs = []
  if isIDSA:
    print('do nothing')
  else:
    for typeEndpoint in typeEndpoints:
      attribList = requests.get(typeEndpoint['url'], headers=typeEndpoint['headers']).json()['attributeDetails']
      for attrib in attribList:
        if attrib['attributeName'] == 'location':
          continue
        value = typeEndpoint['baseUrl'] + ';' + typeEndpoint['type'] + ';' + attrib['attributeName']
        type2Attribs.append({'label': 'Type: ' + typeEndpoint['type'] + ' Attribute: ' + attrib['attributeName'], 'value': value, 'extra': {'url': typeEndpoint['baseUrl'], 'headers': typeEndpoint['headers'], 'type': typeEndpoint['type'], 'attribName': attrib['attributeName'], 'isIDSA': typeEndpoint['isIDSA'], 'atContext': typeEndpoint['atContext']}})
  return type2Attribs
def getEntityTypeAttr(endpoints):
  entryList = []
  for endpoint in endpoints:
    entryList.append({'label': html.H5(endpoint['url']), 'value': endpoint['url'], 'disabled': True})
    if 'atContext' in endpoint:
      atContext = endpoint['atContext']
    else:
     atContext = 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld'
    typeEndpoints = getTypeEndpoints(endpoint['url'], atContext, endpoint['isIDSA'])
    entryList = entryList + getAttribs(typeEndpoints, endpoint['isIDSA'])
  return entryList

def queryEntities(query):
  #currentQueries.append({'qId': queryName, 'url': type2Attr['url'], 'atContext': type2Attr['atContext'], 'isIDSA': type2Attr['isIDSA'],  'type2Attrib': type2Attr['type'] + ' ' + type2Attr['attribName'], 'q': q, 'geoQ': geoQ, 'headers': type2Attr['headers']})
  if query['isIDSA']:
    print('do nothing')
    return []
  else:
    url = query['url']+'/ngsi-ld/v1/entities?type='+urllib.parse.quote(query['type'], safe='')
    if query['q'] and len(query['q'])>0:
      url += '&q=' + urllib.parse.quote(query['q'], safe='')
    if query['geoQ'] and len(query['geoQ'])>0:
      url += '&' + urllib.parse.quote(query['geoQ'], safe='')
    return requests.get(url, headers=query['headers']).json()
  
def getEntities(currentQueries):
  entities = {}
  temp = {}
  for entry in currentQueries:
    qId = entry['qId']
    entities[qId] = []
    for entity in queryEntities(entry):
      entities[qId].append(entity)
  return entities

def getInitialMap():
  dummy = {'_': [], 'lon':[], 'lat':[]}
  df = pd.DataFrame(dummy)
  result = px.scatter_mapbox(df, lat='lat', lon='lon', hover_name='_', zoom=3, color='_', mapbox_style='carto-darkmatter', template='plotly_dark')
  result.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, uirevision = 'something')#, paper_bgcolor='black', legend_font_color='white')
  return result

@app.callback(
  Output('hosts-collabsable', 'is_open'),
  Output('show-hide-hosts-button', 'children'),
  Input('show-hide-hosts-button', 'n_clicks'),
  State('hosts-collabsable', 'is_open'))
def toggleCollapseHosts(n, isOpen):
  if n and n > 0:
    if isOpen:
      return not isOpen, '⮞Hosts⮜'
    else:
      return not isOpen, '⮟Hosts⮟'
  return showHosts, '⮟Hosts⮟'

@app.callback(
  Output('queries-collabsable', 'is_open'),
  Output('show-hide-queries-button', 'children'),
  Input('show-hide-queries-button', 'n_clicks'),
  State('queries-collabsable', 'is_open'))
def toggleCollapseHosts(n, isOpen):
  if n and n > 0:
    if isOpen:
      return not isOpen, '⮞Queries⮜'
    else:
      return not isOpen, '⮟Queries⮟'
  return showHosts, '⮟Queries⮟'

@app.callback(
  Output('hosts-table', 'data'),
  Input('add-host-button', 'n_clicks'),
  State('host-input', 'value'),
  State('hosts-table', 'data'),
  State('atcontext-input', 'value'),
  State('idsa-check', 'value'))
def addHost(n, host, currentHosts, atContext, idsaCheck):
  if n and n > 0:
    isIDSA = (idsaCheck is not None and 'isIDSA' in idsaCheck)
    currentHosts.append({'url': host, 'atContext': atContext, 'isIDSA': isIDSA})
  return currentHosts
    
@app.callback(
  Output('type2attrs-dropdown', 'options'),
  Output('type2attrs-dropdown_container', 'hidden'),
  Input('hosts-table', 'data'))
def checkHosts(endpoints):
  if endpoints and len(endpoints) > 0:
    return getEntityTypeAttr(endpoints), ''
  else:
    return [],'hidden'
    
@app.callback(
  Output('query-table', 'data'),
  Input('add-query-button', 'n_clicks'),
  State('query-name', 'value'),
  State('q-input', 'value'),
  State('geoq-input', 'value'),
  State('type2attrs-dropdown', 'options'),
  State('type2attrs-dropdown', 'value'),
  State('query-table', 'data'))
def addQuery(n, queryName, q, geoQ, type2AttrOptions, selectedType2Attr, currentQueries):
  if n and n > 0:
    if not selectedType2Attr or len(selectedType2Attr) == 0:
      return currentQueries
    if not queryName or queryName == '':
      queryName = str(random.randint(0, 100000))
    for entry in currentQueries:
      if entry['qId'] == queryName:
        return currentQueries
    for entry in type2AttrOptions:
      if entry['value'] == selectedType2Attr:
        type2Attr = entry['extra']
    #'extra': {'url': typeEndpoint['baseUrl'], 'headers': typeEndpoint['headers'], 'type': typeEndpoint['type'], 'attribName': attrib['attributeName'], 'isIDSA': typeEndpoint['isIDSA'], 'atContext': typeEndpoint['atContext']}
    currentQueries.append({'qId': queryName, 'url': type2Attr['url'], 'atContext': type2Attr['atContext'], 'isIDSA': type2Attr['isIDSA'],  'type2Attrib': type2Attr['type'] + ' ' + type2Attr['attribName'], 'q': q, 'geoQ': geoQ, 'headers': type2Attr['headers'], 'attribName': type2Attr['attribName'], 'type': type2Attr['type']})
    print(json.dumps(currentQueries))
  return currentQueries

@app.callback(
    Output('interval-component', 'disabled'),
    Output('entities_view_container', 'hidden'),
    Output('live_graph_container', 'hidden'),
    Output('type2attrs-dropdown', 'value'),
    Output('interval-component', 'n_intervals'),
    Input('query-table', 'data'))
def startStopQueries(queryTableData):
  if not queryTableData or len(queryTableData) <= 0:
    return True, 'hidden', 'hidden', '', 0
  return False, '', '', '', 1
    


@app.callback(
    Output('entities-table', 'data'),
    Output('value_distribution_graph', 'figure'),
    Output('min_max_avg_graph', 'figure'),
    Output('entities-map', 'figure'),
    Input('interval-component', 'n_intervals'),
    State('query-table', 'data'))
def intervalGetting(n, queryTable):
  if not n or n <=0 or not queryTable or len(queryTable) == 0:
    return [], px.bar(title='Value Distribution', barmode='group', template='plotly_dark'), px.bar(title='Minimum, Maximum, Average', barmode='group', template='plotly_dark'), getInitialMap()
  entitiesTable = []
  temp = {}
  temp2 = {}
  mapLayers = {}
  indexes = set()
  currentEntities = getEntities(queryTable)  
  for query in queryTable:
    qId = query['qId']
    attribName = query['attribName']
    entities = currentEntities[qId]
    temp[qId] = {}
    temp2[qId] = {}
    temp2[qId]['min'] = 9999999999999999999
    temp2[qId]['max'] = -9999999999999999999
    temp2[qId]['all'] = 0
    temp2[qId]['count'] = 0
    mapLayers[qId] = {'attribName': attribName, 'mapData': {'entityId': [], attribName: [], 'lon':[], 'lat':[], 'unitCode':[]}}
    for entity in entities:
      entityId = entity['id']
      if attribName in entity:
        if 'object' in entity[attribName]:
          attribValue = entity[attribName]['object']
        else:
          attribValue = entity[attribName]['value']
        if type(attribValue) == float or type(attribValue) == int:
          index = round(attribValue, ROUNDINGVALUE)
          if attribValue > temp2[qId]['max']:
            temp2[qId]['max'] = attribValue
          if attribValue < temp2[qId]['min']:
            temp2[qId]['min'] = attribValue
          temp2[qId]['all'] += attribValue
          temp2[qId]['count'] += 1
        else:
          index = str(attribValue)
        indexes.add(index)
        if not index in temp[qId]:
          temp[qId][index] = 0
        temp[qId][index] += 1
        entitiesTable.append({'entityId': entityId, 'entityType': entity['type'], 'attrib': attribName, 'attrib_value': str(attribValue), 'query': query})
        if 'location' in entity and entity['location']['value']['type'] == 'Point':
          mapLayers[qId]['mapData']['entityId'].append(entityId)
          mapLayers[qId]['mapData'][attribName].append(attribValue)
          mapLayers[qId]['mapData']['lon'].append(entity['location']['value']['coordinates'][0])
          mapLayers[qId]['mapData']['lat'].append(entity['location']['value']['coordinates'][1])
          if 'unitCode' in entity[attribName]:
            mapLayers[qId]['mapData']['unitCode'].append(entity[attribName]['unitCode'])
          else:
            mapLayers[qId]['mapData']['unitCode'].append('')
  indexes = list(indexes)
  indexes.sort()
  figureData = {}
  minMaxData = {}
  for colName, colEntries in temp.items():
    colValues = []
    for index in indexes:
      if index in colEntries:
        colValues.append(colEntries[index])
      else:
        colValues.append(0)
    figureData[colName] = colValues
    if temp2[colName]['count'] > 0:
      minMaxData[colName] = [temp2[colName]['min'], temp2[colName]['max'], temp2[colName]['all']/temp2[colName]['count']]
  df = pd.DataFrame(data=figureData, index=indexes)
  df2 = pd.DataFrame(data=minMaxData, index=['Min', 'Max', 'Average'])
  barChart = px.bar(df, title='Value Distribution', barmode='group', template='plotly_dark')
  barChart.update_layout(uirevision = 'something')#, paper_bgcolor='grey', plot_bgcolor='darkgrey')
  minMaxChart = px.bar(df2, title='Minimum, Maximum, Average', barmode='group', template='plotly_dark')
  minMaxChart.update_xaxes(title=None, visible=True, showticklabels=True)
  minMaxChart.update_layout(uirevision = 'something')#, paper_bgcolor='grey', plot_bgcolor='darkgrey')
  entitiesMap = None
  indexCounter = 0
  temp = {}
  attribColumnNames = []
  legendNames = []
  indexes = []
  temp['lon'] = []
  temp['lat'] = []
  temp['unitCode'] = []
  temp['entityId'] = []
  for qId, entry in mapLayers.items():
    attribName = entry['attribName']
    mapData = entry['mapData']
    attribData = []
    attribIndex = []
    attribColumnNames.append(qId + ' ' + attribName)
    legendNames.append(attribName + ' ' + mapData['unitCode'][0])
    for mapDataEntry in mapData[attribName]:
      attribData.append(mapDataEntry)
      attribIndex.append(indexCounter)
      indexes.append(indexCounter)
      indexCounter += 1
    temp[qId + ' ' + attribName] = pd.Series(attribData, index=attribIndex)
    temp['lon'] = temp['lon'] + mapData['lon']
    temp['lat'] = temp['lat'] + mapData['lat']
    temp['entityId'] = temp['entityId'] + mapData['entityId']
    temp['unitCode'] = temp['unitCode'] + mapData['unitCode']
  temp['lon'] = pd.Series(temp['lon'], index=indexes)
  temp['lat'] = pd.Series(temp['lat'], index=indexes)
  temp['entityId'] = pd.Series(temp['entityId'], index=indexes)
  temp['unitCode'] = pd.Series(temp['unitCode'], index=indexes)
  
  
  df = pd.DataFrame(data=temp, index=indexes)
  entitiesMap = px.scatter_mapbox(df, lat='lat', lon='lon', zoom=3, mapbox_style='carto-darkmatter', template='plotly_dark', hover_name='entityId')
  entitiesMap.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, uirevision = 'something', legend=dict(yanchor="top",y=0.99,xanchor="left",x=0.01))
  for i in range(len(attribColumnNames)):
    attribColumnName = attribColumnNames[i]
    temp = px.scatter_mapbox(df, lat='lat', lon='lon', hover_name='entityId', zoom=3, color=attribColumnName, mapbox_style='carto-darkmatter', template='plotly_dark')
    entitiesMap.add_scattermapbox(marker=temp.data[0].marker, hovertext=temp.data[0].hovertext, hovertemplate=temp.data[0].hovertemplate, mode=temp.data[0].mode, subplot=temp.data[0].subplot, lat=temp.data[0].lat, lon=temp.data[0].lon, name=attribColumnName)
  return entitiesTable, barChart, minMaxChart, entitiesMap


@app.callback(
    Output('history-line-graph', 'figure'),
    Output('history-line-graph-container', 'hidden'),
    Input('entities-table', 'derived_virtual_selected_rows'),
    Input('entities-table', 'derived_virtual_data'))
def history(rows, data):
  if rows == None:
    rows = []
  lineData = {}
  for i in rows:
    entity = temporalGet(data[i]['query']['url'], data[i]['query']['headers'], data[i]['query']['isIDSA'], data[i]['entityId'], data[i]['attrib'])
    attribData = []
    attribIndex = []
    for entry in entity[data[i]['attrib']]:
      attribData.append(entry['value'])
      attribIndex.append(pd.to_datetime(entry['modifiedAt']))
    lineData[data[i]['query']['qId'] + ' ' + data[i]['entityId']] = pd.Series(attribData, index=attribIndex)
  df = pd.DataFrame(lineData)
  result = px.line(df, template='plotly_dark' )
  result.update_traces(connectgaps=True)
  result.update_layout(uirevision = 'something', paper_bgcolor='#222222')
  if len(rows) == 0:
    return result, 'hidden'
  else:
    return result, ''


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050)
