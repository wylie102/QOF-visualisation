"""Layout components and configuration for QOF visualization.

This module provides functions for creating the various UI components of the QOF
visualization dashboard. It includes components for the header, control bar,
description area, and main visualization area.

Each component is a function that returns a Dash HTML component or Graph.
The components are designed to be composed together into a complete dashboard layout.

Typical usage example:
    layout = create_app_layout(
        default_year=2024,
        default_ind="BP002"
    )
"""

from dash import dcc, html

from QOF_visualisation.visualization.constants import (
    BUCKET_SQL,
    DEFAULT_BUCKET,
    ORG_TABLE,
)


def create_header() -> html.H3:
    """Create the header component.

    Returns:
        A Dash H3 component containing the dashboard title.
    """
    return html.H3("QOF Performance Map")


def create_control_bar(
    default_year: int | None = None,
    default_ind: str | None = None,
) -> html.Div:
    """Create the control bar with dropdowns and radio buttons.

    Args:
        default_year: Default selected year for the year dropdown.
        default_ind: Default selected indicator for the indicator dropdown.

    Returns:
        A Dash Div component containing:
            - Year dropdown
            - Indicator dropdown
            - Organization level radio buttons
            - Achievement bucket radio buttons
    """
    return html.Div(
        [
            dcc.Dropdown(
                id="yr",
                style={
                    "width": 110,
                    "minWidth": 110,
                    "maxWidth": 110,
                    "marginLeft": 8,
                    "marginRight": 8,
                },
                value=default_year,
            ),
            dcc.Dropdown(
                id="ind",
                style={"width": 180, "minWidth": 180, "maxWidth": 180},
                value=default_ind,
            ),
            dcc.RadioItems(
                id="level",
                options=[{"label": k, "value": k} for k in ORG_TABLE],
                value="Practice",
                inline=True,
                style={"paddingLeft": 16},
            ),
            dcc.RadioItems(
                id="bucket",
                options=[{"label": k, "value": k} for k in BUCKET_SQL],
                value=DEFAULT_BUCKET,
                inline=True,
                style={"paddingLeft": 16},
            ),
        ],
        style={"display": "flex", "gap": 6, "alignItems": "center"},
    )


def create_description() -> html.Div:
    """Create the description component.

    Returns:
        A Dash Div component that will contain the description text.
        The content is populated dynamically based on selected indicators.
    """
    return html.Div(id="desc", style={"fontSize": 14, "marginTop": 6})


def create_visualization_area() -> html.Div:
    """Create the main visualization area with map and bar chart.

    Returns:
        A Dash Div component containing:
            - Map graph showing organization locations (60% width)
            - Bar chart showing achievement comparisons (38% width)
        The graphs are arranged side by side with a 1% gap between them.
    """
    return html.Div(
        [
            dcc.Graph(
                id="map",
                style={"height": "82vh", "width": "60vw"},
                config={"scrollZoom": True},
            ),
            html.Div(
                dcc.Graph(
                    id="bars",
                    style={"width": "100%", "height": "100%"},
                    config={"scrollZoom": False},
                ),
                style={
                    "width": "38vw",
                    "height": "82vh",
                    "overflowY": "auto",
                    "marginTop": "0vh",
                },
            ),
        ],
        style={"display": "flex", "gap": "1%"},
    )


def create_app_layout(default_year: int | None = None, default_ind: str | None = None) -> html.Div:
    """Create the main application layout.

    Args:
        default_year: Default selected year for the year dropdown.
        default_ind: Default selected indicator for the indicator dropdown.

    Returns:
        A Dash Div component containing the complete dashboard layout:
            1. Header with title
            2. Control bar with filters
            3. Description area
            4. Main visualization area (map and chart)
    """
    return html.Div(
        [
            create_header(),
            create_control_bar(default_year, default_ind),
            create_description(),
            create_visualization_area(),
        ],
        style={"padding": 12},
    )
