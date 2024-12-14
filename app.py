from pandas import read_csv
from dash import dcc, Input, Output, html, Dash
import plotly.graph_objects as go
from dash_bootstrap_components.themes import BOOTSTRAP
from dash_bootstrap_components import Row, Col
from ttracker.plotting_tools import plot_map
from ttracker.system import System
# Memory monitoring function

# Initialize the Dash app
app = Dash(__name__, external_stylesheets=[BOOTSTRAP], update_title=None)
server = app.server
app.title = "T Tracker"
# prepare data
mbta_system = System("./static/data/clean/stations.csv",
                     "./static/data/clean/links.csv",
                     "./static/data/clean/stop_codes_to_station_id_crosswalk.csv",
                     "https://cdn.mbta.com/realtime/VehiclePositions.pb")
stations_df = mbta_system.station_data.copy()
stations_df.loc[:, 'map_size'] = stations_df['map_color'].replace({'black': 5,
                                                                   'red': 10,
                                                                   'green': 10,
                                                                   'blue': 10,
                                                                   'orange': 10})
links_df = mbta_system.links_data.copy()
charles_river_df = read_csv("./static/data/clean/charles_river.csv")

# Create figure object
base_figure = go.Figure(layout={'dragmode': False})
plot_map("mbta", base_figure, links_df, stations_df, charles_river_df)

# Layout of the app
app.css.config.serve_locally = True
app.layout = html.Div(children=[
    html.Center([html.H1("ttracker.io", style={
        "position": "fixed",
        "top": 0,
        "left": 0,
        "right": 0,
        'justify-content': 'center',
        'align-items': 'center',

        "background-color": "white",
        'display': 'inline-block'
    })]
                     ),
    html.Div(children=[Row([
        Col([dcc.Graph(id='train-map',
                           figure=base_figure,
                           style={
                               'border': '2px solid black',
                               'padding': '10px',
                               'max-width': '100vw',  # Ensure the graph doesn't overflow the viewport
                               'max-height': '100vh',  # Ensure the graph doesn't overflow the viewport
                               'width': '100%',  # Graph takes 100% of the available width
                               'height': '100%'  # Graph takes 100% of the available height
                           },
                           config={'modeBarButtonsToRemove': ['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'zoomIn2d',
                                                              'zoomOut2d', 'autoScale2d', 'resetScale2d'],
                                   'displayModeBar': False,  # Disable mode bar
                                   'scrollZoom': False,  # Disable zooming with mouse scroll
                                   'showTips': False,  # Disable tips
                                   'displaylogo': False  # Disable plotly logo

                                   }),
                 ]),
        dcc.Interval(id='interval-component', interval=4000, n_intervals=0)  # Update every second
    ])], style={
        'display': 'flex',  # Enable flexbox
        'justify-content': 'center',  # Center horizontally
        'align-items': 'center',  # Center vertically
        'height': 'calc(100vh - 50px)',  # Subtract the height of the title (estimate 120px)
    })

])


# Callback to update train positions
@app.callback(
    Output('train-map', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_train_positions(n_intervals):
    new_x, new_y, color, hover_text = mbta_system.update_trains()

    # Create a new figure based on the base map
    updated_figure = base_figure
    updated_figure['data'] = updated_figure['data'][:-1]


    # Add train positions to the map
    updated_figure.add_trace(
        go.Scatter(
            x=new_x,
            y=new_y,
            mode='markers',
            marker=dict(color=color, size=5, line=dict(
                        color='black',  # Outline color
                        width=1  # Outline width
                    )),
            name='Trains',
            text=hover_text,
            hoverinfo='text',
            showlegend=False
        )
    )

    return updated_figure


# Run the app
if __name__ == '__main__':
    app.run_server(debug=False)
