from typing import List
import numpy as np
import logging

logger = logging.getLogger(__name__)

def segregate_outliers(value_counts, outlier_value):
    """
    Identifies outliers in a cluster based on the cluster ID.
    Returns a list of outlier cluster Indicies and 
    removes them from the cluster counts.
    """
    
    outliers = value_counts[value_counts.index == outlier_value].index
    outliers = set(list(outliers)) # remove duplicates
    new_counts = value_counts[value_counts.index != -1] # drop outliers
    return outliers, new_counts

            
def remove_from_df(df, list_of_ids: List[int], field: str):
    """
    removes rows from a dataframe based on a list of IDs.
    """
    return df[~df[field].isin(list_of_ids)]  

def locate_in_df(df, list_of_ids: List[int], field: str):
    """
    locates rows in a dataframe based on a list of IDs.
    """
    return df[df[field].isin(list_of_ids)]


def generate_cluster_string(List:[str]) -> List[str]:
    """
    Generates and assigns cluster IDs field to df. Returns df with cluster_ID field.
    """
    cluster_string = '-'.join(List)
    return cluster_string


def num_2_short_form(number):
    """
    Short form text creation for 
    display purposes
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
    for key, value in kwargs.items():
        df[key] = value
    return df