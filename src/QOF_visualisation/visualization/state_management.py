"""State management and callback logic for QOF visualization.

This module handles state management and data preparation for the QOF visualization
dashboard. It includes functions for managing achievement bucket selections and
preparing data for visualization.

Typical usage example:
    enabled_buckets = get_enabled_buckets(bucket_options)
    selected_bucket = select_bucket_value(
        bucket_opts=bucket_options,
        current_bucket="80-100%",
        default_bucket="60-80%"
    )
    plot_data, groups = prepare_plot_data(achievement_data)
"""

from collections.abc import Mapping, Sequence
from typing import NamedTuple, TypedDict

import polars as pl


# Type definitions for improved clarity
class BucketOption(TypedDict):
    """Type definition for bucket option dictionaries.

    Attributes:
        label: Display label for the bucket option
        value: Internal value for the bucket option
        disabled: Whether the bucket option is disabled
    """

    label: str
    value: str
    disabled: bool | None  # None means not disabled


class PlotData(NamedTuple):
    """Container for plot data and groupings.

    Attributes:
        data: Dictionary containing plot data series
        groups: List of group labels for plotting
    """

    data: Mapping[str, Sequence[float | str]]
    groups: list[str]


def get_enabled_buckets(bucket_opts: list[BucketOption]) -> list[BucketOption]:
    """Get list of enabled bucket options.

    Args:
        bucket_opts: List of bucket options with their enabled/disabled state.

    Returns:
        List of bucket options that are not disabled.
    """
    return [opt for opt in bucket_opts if not opt.get("disabled", False)]


def select_bucket_value(
    bucket_opts: list[BucketOption],
    current_bucket: str | None,
    default_bucket: str,
) -> str | None:
    """Select appropriate bucket value based on current state and available options."""
    enabled_buckets = get_enabled_buckets(bucket_opts)

    # Try to keep current bucket if valid
    if current_bucket and any(
        opt["value"] == current_bucket and not opt.get("disabled", False) for opt in bucket_opts
    ):
        return current_bucket

    # Try default bucket if available
    if any(
        opt["value"] == default_bucket and not opt.get("disabled", False) for opt in bucket_opts
    ):
        return default_bucket

    # Fall back to first enabled bucket
    if enabled_buckets:
        return str(enabled_buckets[0]["value"])

    return None


def prepare_plot_data(combined_df: pl.DataFrame) -> PlotData:
    """Prepare data for plotting by handling None values and sorting.

    Args:
        combined_df: DataFrame containing organization and national achievement data.
            Must have columns: group_description, org_achievement, nat_achievement.

    Returns:
        PlotData containing:
        - data: Dictionary with plot data series (groups and achievement values)
        - groups: List of sorted group descriptions
    """
    # Get non-None group descriptions and sort
    groups = [g for g in combined_df["group_description"].to_list() if g is not None]
    groups = sorted(groups, reverse=True)

    # Create plot data structure
    plot_data = {
        "Group": groups * 2,
        "Achievement": [
            combined_df.filter(pl.col("group_description") == g)["org_achievement"].item()
            for g in groups
        ]
        + [
            combined_df.filter(pl.col("group_description") == g)["nat_achievement"].item()
            for g in groups
        ],
    }

    return PlotData(data=plot_data, groups=groups)
