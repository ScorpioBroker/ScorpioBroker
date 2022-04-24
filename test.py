import pandas as pd
from dash.dependencies import Input, Output
us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")

import plotly.express as px
import plotly.graph_objects as go

fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=800)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig['layout']['uirevision'] = 'something'

import dash
#import dash_core_components as dcc
#import dash_html_components as html
from dash import dcc
from dash import html


#g = go.FigureWidget(fig)
#def handle_zoom(layout, mapbox_zoom):
#    print('new mapbox_zoom:', mapbox_zoom)
#g.layout.on_change(handle_zoom, 'mapbox_zoom')


app = dash.Dash()
app.layout = html.Div([
    dcc.Graph(id='bla', figure=fig)
])
@app.callback(Output("bla", "figure"),[Input("bla", "relayoutData"), Input("bla", "selectedData"), Input("bla", "hoverData"), Input("bla", "clickData")])
def display_choropleth(relay, selected, hover, click):
  print(str(type(relay)))
  print(str(relay))
  print(str(type(selected)))
  print(str(selected))
  print(str(type(hover)))
  print(str(hover))
  print(str(type(click)))
  print(str(click))
  print("##################################")
  #fig.update_layout(pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k-multi-year.csv"))
  return fig


app.run_server(debug=True, use_reloader=True)





