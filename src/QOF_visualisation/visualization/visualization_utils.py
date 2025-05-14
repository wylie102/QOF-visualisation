"""Visualization utilities for creating maps and charts.

This module provides functions for creating interactive visualizations using Plotly,
including maps with organization markers and bar charts comparing performance data.

Typical usage example:
    # Create a map with organization markers
    fig = create_map(
        df=achievement_data,
        center_lat=54.5,
        center_lon=-2,
        zoom=6.0
    )

    # Create a comparison bar chart
    fig = create_bar_chart(
        df=comparison_data,
        org_name="Example Practice",
        title="QOF Achievement Comparison"
    )
"""

import plotly.graph_objects as go
import polars as pl


def create_map(
    df: pl.DataFrame,
    center_lat: float = 54.5,
    center_lon: float = -2,
    zoom: float = 6.0,
) -> go.Figure:
    """Create a map visualization with practice/organization markers.

    Args:
        df: DataFrame containing lat, lng, organisation_name, and pct columns.
        center_lat: Latitude for the center of the map (default: 54.5).
        center_lon: Longitude for the center of the map (default: -2).
        zoom: Initial zoom level for the map (default: 6.0).

    Returns:
        A Plotly Figure object containing the map visualization.

    The map shows organization locations with markers that display
    the name and achievement percentage on hover.
    """
    fig = go.Figure(
        data=[
            go.Scattermap(
                lat=df["lat"].to_list(),
                lon=df["lng"].to_list(),
                mode="markers",
                marker=dict(
                    size=6,
                    color="#1f77b4",
                    opacity=0.95,
                ),
                hoverinfo="text",
                text=[
                    f"<b>{name}</b><br>{pct:.1f}%"
                    for name, pct in zip(
                        df["organisation_name"].to_list(), df["pct"].to_list(), strict=True
                    )
                ],
                customdata=list(
                    zip(df["pct"].to_list(), df["organisation_name"].to_list(), strict=True)
                ),
            )
        ]
    )

    fig.update_layout(
        margin=dict(t=0, r=0, l=0, b=0),
        height=700,
        uirevision="constant",
        mapbox=dict(
            style="carto-positron",
            zoom=zoom,
            center=dict(lat=center_lat, lon=center_lon),
        ),
    )

    return fig


def create_bar_chart(
    df: pl.DataFrame,
    org_name: str,
    title: str | None = None,
) -> go.Figure:
    """Create a horizontal bar chart comparing organization vs national averages.

    Args:
        df: DataFrame containing Source, Achievement, and Group columns.
        org_name: Name of the organization being compared.
        title: Optional custom title for the chart.
            If None, defaults to "Performance Comparison - {org_name}".

    Returns:
        A Plotly Figure object containing the bar chart visualization.

    The chart shows side-by-side bars comparing the organization's achievement
    percentages against national averages across different indicator groups.
    """
    bar_fig = go.Figure()

    # Add organization data
    bar_fig.add_trace(
        go.Bar(
            x=df.filter(pl.col("Source") == org_name)["Achievement"].to_list(),
            y=df.filter(pl.col("Source") == org_name)["Group"].to_list(),
            orientation="h",
            name=org_name,
            marker_color="#1f77b4",
            hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>",
            hoverlabel=dict(bgcolor="white", font=dict(size=12), bordercolor="#1f77b4"),
        )
    )

    # Add national average data
    bar_fig.add_trace(
        go.Bar(
            x=df.filter(pl.col("Source") == "National Average")["Achievement"].to_list(),
            y=df.filter(pl.col("Source") == "National Average")["Group"].to_list(),
            orientation="h",
            name="National Average",
            marker_color="#ff7f0e",
            hovertemplate="%{y}<br>%{x:.1f}%<extra></extra>",
            hoverlabel=dict(bgcolor="white", font=dict(size=12), bordercolor="#ff7f0e"),
        )
    )

    title = title or f"Performance Comparison - {org_name}"

    # Update layout
    bar_fig.update_layout(
        barmode="group",
        bargroupgap=0.3,
        bargap=0.15,
        margin=dict(l=200, r=50, t=100, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=800,
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(0,0,0,0.1)",
            title=dict(
                text="Achievement (%)",
                font=dict(size=12),
                standoff=10,
            ),
            range=[0, 100],
            zeroline=True,
            zerolinecolor="rgba(0,0,0,0.2)",
            zerolinewidth=1,
        ),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.1)", tickfont=dict(size=11)),
        hoverdistance=100,
        hovermode="closest",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1,
        ),
        title=dict(
            text=title,
            x=0.5,
            xanchor="center",
            y=0.95,
            font=dict(size=14),
        ),
    )

    return bar_fig


def create_blank_map(
    center_lat: float = 53,
    center_lon: float = -1.5,
    zoom: float = 5,
) -> go.Figure:
    """Create an empty map figure."""
    fig = go.Figure()
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center={"lat": center_lat, "lon": center_lon},
            zoom=zoom,
        ),
        margin=dict(t=0, r=0, l=0, b=0),
        height=700,
    )
    return fig


def create_blank_bar(msg: str = "Click a point") -> go.Figure:
    """Create an empty bar chart figure with message."""
    fig = go.Figure()
    fig.update_layout(
        title=msg,
        xaxis=dict(visible=True, range=[0, 110], title="% Achieved"),
        yaxis=dict(visible=False),
        margin=dict(t=40, r=10, l=10, b=40),
        showlegend=False,
    )
    return fig
