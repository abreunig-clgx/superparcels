from typing import List
import numpy as np
import logging
import multiprocessing

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


def create_batches(arg_tuples, batch_size):
    """
    Split a list of argument tuples into batches of a specified size.

    This generator function yields batches (sublists) of argument tuples for processing, each with a length 
    equal to 'batch_size' (except possibly the last batch).

    Parameters:
        arg_tuples (list): A list of argument tuples.
        batch_size (int): The desired number of tuples in each batch.

    Yields:
        list: A batch (sublist) of argument tuples.
    """
    for i in range(0, len(arg_tuples), batch_size):
        yield arg_tuples[i:i + batch_size]


def mp_framework(func, arg_tuples, n_jobs=None):
    """
    Execute a function in parallel using multiprocessing.

    This function provides a multiprocessing framework to execute the specified function in parallel across multiple 
    processes. It uses a process pool to apply the function to each tuple of arguments in 'arg_tuples' via starmap.

    Parameters:
        func (callable): The function to be executed in parallel.
        arg_tuples (list of tuples): A list of argument tuples to pass to the function.
        n_jobs (int, optional): The number of parallel processes to use. If None, the default is used.

    Returns:
        None
    """
    
    with multiprocessing.Pool(processes=n_jobs) as pool:
        results = pool.starmap(func, arg_tuples)

    return results

def add_attributes(df, **kwargs):
    for key, value in kwargs.items():
        df[key] = value
    return df