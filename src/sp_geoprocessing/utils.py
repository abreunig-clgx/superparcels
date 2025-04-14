from typing import List
import logging

logger = logging.getLogger(__name__)


def segregate_outliers(value_counts, outlier_value):
    """
    Identify outlier clusters and separate them from the normal clusters.

    This function examines a pandas Series of cluster counts, identifies the outlier cluster(s) 
    based on the provided outlier_value (typically -1), and returns a set of outlier indices 
    along with a new Series excluding those outlier counts.

    Parameters
    ----------
    value_counts : pandas.Series
        A Series where the index represents cluster IDs and values represent the count of elements in each cluster.
    outlier_value : int
        The value representing an outlier cluster (often -1).

    Returns
    -------
    tuple
        A tuple containing:
            - set: A set of outlier cluster indices.
            - pandas.Series: A Series of cluster counts excluding the outlier clusters.
    """
    outliers = value_counts[value_counts.index == outlier_value].index
    outliers = set(list(outliers))  # Remove duplicates
    new_counts = value_counts[value_counts.index != -1]  # Drop outliers (assumed to be -1)
    return outliers, new_counts


def remove_from_df(df, list_of_ids: List[int], field: str):
    """
    Remove rows from a DataFrame based on a list of IDs for a specified column.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    list_of_ids : list of int
        A list of IDs that should be removed from the DataFrame.
    field : str
        The name of the column in which to check for the IDs.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with rows where the 'field' has values in list_of_ids removed.
    """
    return df[~df[field].isin(list_of_ids)]  


def locate_in_df(df, list_of_ids: List[int], field: str):
    """
    Locate and return rows in a DataFrame based on a list of IDs for a specified column.

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame.
    list_of_ids : list of int
        A list of IDs to locate in the DataFrame.
    field : str
        The name of the column in which to search for the IDs.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame containing only rows where the 'field' contains values in list_of_ids.
    """
    return df[df[field].isin(list_of_ids)]


def generate_cluster_string(List: str) -> List[str]:
    """
    Generate a single cluster identifier string by concatenating a list of strings.

    This function combines elements in the provided list with hyphens to form a unique cluster 
    identifier. This is useful for assigning or naming cluster IDs in a DataFrame.

    Parameters
    ----------
    List : list of str
        A list of string components to be joined.

    Returns
    -------
    str
        A single string representing the concatenated cluster identifier.
    """
    cluster_string = '-'.join(List)
    return cluster_string


def num_2_short_form(number):
    """
    Convert a number to a human-friendly short form for display purposes.

    The function converts numerical values into abbreviated form with suffixes:
      - 'B' for billions,
      - 'M' for millions,
      - 'k' for thousands.
    For numbers below 1,000, the number is returned as a string.

    Parameters
    ----------
    number : int or float
        The number to convert.

    Returns
    -------
    str
        A string representing the shortened form of the number.
    """
    if number >= 1_000_000_000:
        return f'{number/1_000_000_000:.1f}B'
    elif number >= 1_000_000:
        return f'{number/1_000_000:.1f}M'
    elif number >= 1_000:
        return f'{number/1_000:.1f}k'
    else:
        return str(number)


def add_attributes(df, **kwargs):
    """
    Add new attribute columns to a DataFrame.

    This function updates the input DataFrame with new columns provided via keyword arguments. 
    Each key in kwargs becomes a new column in the DataFrame with the corresponding value assigned.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to which new attributes will be added.
    **kwargs : dict
        Arbitrary keyword arguments where keys are the column names and values are the column values 
        to be added to the DataFrame.

    Returns
    -------
    pandas.DataFrame
        The updated DataFrame with the new attributes added.
    """
    for key, value in kwargs.items():
        df[key] = value
    return df
