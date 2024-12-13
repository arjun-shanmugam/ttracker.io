from typing import List

import pandas as pd
import plotly.graph_objects as go


def plot_map(style: str,
             figure: go.Figure,
             links_df: pd.DataFrame,
             stations_df: pd.DataFrame,
             charles_river_df: pd.DataFrame):
    if style == "mbta":

        # Add charles river
        figure.add_trace(
            go.Scatter(
                x=charles_river_df['x'],
                y=charles_river_df['y1'],
                mode='lines',
                line=dict(color='rgb(145,193,217)'),
                showlegend=False
            ))
        figure.add_trace(go.Scatter(
            x=charles_river_df['x'],
            y=charles_river_df['y2'],
            mode='lines',
            fill='tonexty',  # Fills between y2 and the previous trace (y1)
            line=dict(color='rgb(145,193,217)'),
            fillcolor='rgb(145,193,217)',
            showlegend=False
        ))

        # Add a triangle using a custom SVG path
        figure.add_shape(
            type="path",
            path="M 214 150 L 214 93 L 272 150 Z",  # Define the triangle (M = move, L = line, Z = close path)
            line=dict(color='rgb(145,193,217)', width=2),  # Outline color and width
            fillcolor='rgb(145,193,217)'
        )

        figure.add_shape(
            # Second rectangle
            type="rect",
            x0=211, x1=214,  # x-coordinates of the rectangle
            y0=50, y1=150,  # y-coordinates of the rectangle
            line=dict(color='rgb(145,193,217)', width=3),
            fillcolor='rgb(145,193,217)',
            layer='below'
        )

        # Add a triangle using a custom SVG path
        figure.add_shape(
            type="path",
            path="M 211 50 L 272 50 L 272 -10 Z",  # Define the triangle (M = move, L = line, Z = close path)
            line=dict(color='rgb(145,193,217)', width=2),  # Outline color and width
            fillcolor='rgb(145,193,217)'
        )

        figure.add_shape(
            # Second rectangle
            type="rect",
            x0=211, x1=272,  # x-coordinates of the rectangle
            y0=50, y1=75,  # y-coordinates of the rectangle
            line=dict(color='rgb(145,193,217)', width=3),
            fillcolor='rgb(145,193,217)'
        )

        figure.add_shape(
            # Second rectangle
            type="rect",
            x0=190, x1=214,  # x-coordinates of the rectangle
            y0=147, y1=150,  # y-coordinates of the rectangle
            line=dict(color='rgb(145,193,217)', width=3),
            fillcolor='rgb(145,193,217)',
            layer='below'
        )

        figure.add_shape(
            # Second rectangle
            type="rect",
            x0=190, x1=192,  # x-coordinates of the rectangle
            y0=147, y1=160,  # y-coordinates of the rectangle
            line=dict(color='rgb(145,193,217)', width=3),
            fillcolor='rgb(145,193,217)',
            layer='below'
        )

        for link in links_df.loc[links_df['direction'] == 0].to_dict('records'):
            link = go.Scatter(x=[link['x_source'], link['x_target']],
                              y=[link['y_source'], link['y_target']],
                              mode='lines',
                              line=dict(color='grey', width=6),
                              showlegend=False,
                              name=f"{link['source_station_id']}_to_{link['target_station_id']}")
            figure.add_trace(link)

            # Add stations
            figure.add_trace(
                go.Scatter(
                    x=stations_df['x'],
                    y=stations_df['y'],
                    mode='markers',
                    text=list(stations_df['name']),
                    hoverinfo='text',
                    textposition='top left',
                    marker=dict(color='grey', size=10),
                    showlegend=False,
                    name='Stations'
                )
            )





    else:
        raise ValueError("Style argument must be \"mbta\".")

    figure.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(
            showgrid=False,  # Hide grid lines
            zeroline=False,  # Hide zero line
            showline=False,  # Hide axis line
            showticklabels=False  # Hide axis tick labels
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=False
        ),

        plot_bgcolor='white'  # Optional: Set the background color to white
    )
