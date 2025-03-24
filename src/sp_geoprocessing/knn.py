import numpy as np
from scipy.spatial import cKDTree
from scipy.ndimage import uniform_filter1d
from scipy.spatial.distance import cdist
from math import ceil
import logging

logger = logging.getLogger(__name__)

""" Functions for KNN distance calculation """      
def calculate_regional_knn_distance(
            coords, 
            kneighbors, 
            smoothing_window, 
            min_distance, 
            max_distance
        ):
    """
    Calculates the optimal distance for DBSCAN clustering. 
    """
    knn_distances = build_knn_distances(coords, k=kneighbors + 1)
    kth_distances = get_kth_distances(knn_distances)
    smoothed_distances = smooth_distances(kth_distances, window=smoothing_window)
    if len(smoothed_distances) <= 2:
        print('Warning: Not enough data to calculate optimal distance. Check input data.')
        return 1
    diff = build_difference(smoothed_distances)
    second_diff = build_difference(diff)
    knn_optimal_distance = calculate_knn_optimal_distance(smoothed_distances, second_diff)

    # optimal distance is between min and max distance
    #return min(max(ceil(knn_optimal_distance), min_distance), max_distance)
    return knn_optimal_distance

def build_knn_distances(coords, k):
    """
    Builds a distance matrix between each parcel and its 1 to kth nearest neighbor.
    Returns a 2d array where each row is a parcel and each column 
    is the distance to itself then 1st, 2nd, ..., kth nearest neighbor.
    """
    dtree = cKDTree(coords) # KDTree for nearest neighbor

    # distances between each parcel and its 1 to kth nearest neighbor
    knn_distances, _ = dtree.query(coords, k=k + 1) # +1 to include distance to itself

    return knn_distances



def get_kth_distances(knn_distances):
    """
    Firsts changes inf to 0, then returns the kth nearest neighbor distance for each parcel.
    Returns the kth nearest neighbor distance for each parcel.
    """
    return np.nan_to_num(knn_distances[:, -1])
    
def smooth_distances(distances, window):
    """
    Smooths distances to reduce noise and make the elbow more apparent.
    Window is size of moving average. 
    """
    return uniform_filter1d(distances, size=ceil(window * len(distances)))

def build_difference(distances):
    """
    Builds the difference between each distance and the next.
    """
    return np.diff(distances)


def calculate_knn_optimal_distance(distances, diff_array):
    """
    Finds the elbow point in the difference array.
    returns the index of the elbow point in the difference array. 
    1 is added to the index to get the kth distance.
    Index is then used to get the optimal distance from a sorted array of distances.
    """
    elbow_index = np.argmax(diff_array) + 1
    knn_dist = distances[elbow_index]
    if knn_dist == np.inf:
        return 1
    if knn_dist <= 0:
        return 1

       
    
    return knn_dist

def merge_small_clusters(labels, centroids, min_cluster_size):
    """
    Merges small clusters into larger clusters.
    Returns the new cluster labels.
    """

    # Step 1: Identify Small Clusters
    cluster_sizes = np.bincount(labels)
    
    small_clusters = np.where(cluster_sizes < min_cluster_size)[0]

    # Step 2: Merge Small Clusters
    for small_cluster in small_clusters:
        small_cluster_indices = np.where(labels == small_cluster)[0]
        small_cluster_centroid = centroids[small_cluster]

        # Find the nearest larger cluster
        other_clusters = [i for i in range(len(centroids)) if i != small_cluster]
        distances = cdist([small_cluster_centroid], centroids[other_clusters], metric='euclidean')
        nearest_cluster = other_clusters[np.argmin(distances)]

        # Reassign small cluster points to the nearest cluster
        labels[small_cluster_indices] = nearest_cluster
    
    return labels