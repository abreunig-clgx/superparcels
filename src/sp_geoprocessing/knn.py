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
    Calculate an optimal distance for DBSCAN clustering based on the k-nearest neighbors (KNN) distances.

    This function computes distances to each parcel's (point's) kth neighbor, applies smoothing to reduce noise,
    computes the first and second differences of the smoothed distances (to find an "elbow" point), and
    returns the optimal distance value. The optimal distance is intended as a candidate epsilon parameter for 
    DBSCAN clustering. If the number of data points is too low, a default value is returned.

    Parameters
    ----------
    coords : array_like
        A 2D array or list of coordinate pairs (e.g., [(x1, y1), (x2, y2), ...]) for which to compute KNN distances.
    kneighbors : int
        The number of nearest neighbors to consider (noting that one extra neighbor is queried to include the point itself).
    smoothing_window : float
        Fractional window size used for smoothing the kth nearest neighbor distance. The actual window 
        size is computed as ceil(window * len(distances)).
    min_distance : float
        The minimum allowable distance (threshold) for the computed optimal distance.
    max_distance : float
        The maximum allowable distance (threshold) for the computed optimal distance.

    Returns
    -------
    float
        The computed optimal distance based on the elbow of the smoothed kth nearest neighbor distances.
        If not enough data points are available, a default value of 1 is returned.
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

    # The following commented code ensures the result is between min and max distance.
    # return min(max(ceil(knn_optimal_distance), min_distance), max_distance)
    return knn_optimal_distance


def build_knn_distances(coords, k):
    """
    Build a distance matrix for each point and its k nearest neighbors using a KDTree.

    This function creates a cKDTree from the input coordinates and queries the tree for the distance 
    from each point to its k nearest neighbors. The returned 2D array contains distances per point, where 
    each row corresponds to one point and the columns contain the distances to its 0th (itself), 1st, 2nd, 
    ..., kth nearest neighbors.

    Parameters
    ----------
    coords : array_like
        A 2D array or list of coordinate pairs (e.g., [(x1, y1), (x2, y2), ...]).
    k : int
        The number of nearest neighbors to query (excluding the point itself). Note that internally 
        k+1 distances are returned since the point itself is included.

    Returns
    -------
    numpy.ndarray
        A 2D array of shape (num_points, k+1) with each row containing distances to the nearest neighbors.
    """
    dtree = cKDTree(coords)  # Build KDTree for the coordinate data
    knn_distances, _ = dtree.query(coords, k=k + 1)  # +1 to include the distance to itself
    return knn_distances


def get_kth_distances(knn_distances):
    """
    Retrieve the kth nearest neighbor distance for each point.

    This function converts any infinite values in the kth neighbor distances to 0, then extracts 
    the kth column (last column) from the KNN distances array, which represents the distance to the 
    kth nearest neighbor for each point.

    Parameters
    ----------
    knn_distances : numpy.ndarray
        A 2D array of distances computed by the build_knn_distances function.

    Returns
    -------
    numpy.ndarray
        A 1D array of the kth nearest neighbor distances for each point.
    """
    return np.nan_to_num(knn_distances[:, -1])


def smooth_distances(distances, window):
    """
    Smooth a 1D array of distances using a uniform (moving average) filter.

    The function uses a smoothing window that is a fraction of the total number of distances, reducing
    high-frequency noise and making the "elbow" in the distance plot more apparent.

    Parameters
    ----------
    distances : array_like
        A 1D array of distances (e.g., kth nearest neighbor distances) to be smoothed.
    window : float
        The fractional window size (between 0 and 1) to determine the actual window length; the actual 
        window is computed as ceil(window * number of distances).

    Returns
    -------
    numpy.ndarray
        A 1D array of smoothed distance values.
    """
    window_size = ceil(window * len(distances))
    return uniform_filter1d(distances, size=window_size)


def build_difference(distances):
    """
    Compute the discrete difference between consecutive distance values.

    This function calculates the first difference of a sorted distance array to capture the rate of 
    change between successive distances.

    Parameters
    ----------
    distances : array_like
        A 1D array of distance values (usually smoothed).

    Returns
    -------
    numpy.ndarray
        A 1D array of differences computed between consecutive distance values.
    """
    return np.diff(distances)


def calculate_knn_optimal_distance(distances, diff_array):
    """
    Determine the optimal distance using the elbow method.

    This function finds the index at which the change in the distance differences (i.e., the second difference) 
    is maximum (the "elbow" point). It then selects the distance corresponding to that index as the optimal 
    distance for clustering.

    Parameters
    ----------
    distances : array_like
        A sorted 1D array of (smoothed) kth nearest neighbor distances.
    diff_array : array_like
        A 1D array representing the first differences of the distances (or second differences if computed externally).

    Returns
    -------
    float
        The optimal distance value derived from the elbow point. Returns a default value of 1 if the derived 
        distance is non-positive or infinite.
    """
    elbow_index = np.argmax(diff_array) + 1  # +1 to adjust index to kth distance
    knn_dist = distances[elbow_index]
    if knn_dist == np.inf or knn_dist <= 0:
        return 1
    return knn_dist


def merge_small_clusters(labels, centroids, min_cluster_size):
    """
    Merge clusters that do not meet a minimum size threshold.

    This function examines cluster labels and identifies clusters with sizes smaller than the 
    specified minimum. For each small cluster, it reassigns its points to the nearest larger cluster 
    based on Euclidean distance between cluster centroids.

    Parameters
    ----------
    labels : numpy.ndarray
        An array of cluster labels assigned to each data point.
    centroids : numpy.ndarray
        An array of cluster centroids, where each row represents the centroid coordinates of a cluster.
    min_cluster_size : int
        The minimum number of points required for a cluster. Clusters with fewer points will be merged 
        with the nearest larger cluster.

    Returns
    -------
    numpy.ndarray
        An array of updated cluster labels after merging small clusters.
    """
    # Identify clusters that are smaller than the minimum required size
    cluster_sizes = np.bincount(labels)
    small_clusters = np.where(cluster_sizes < min_cluster_size)[0]

    # Merge each small cluster into its nearest larger cluster
    for small_cluster in small_clusters:
        small_cluster_indices = np.where(labels == small_cluster)[0]
        small_cluster_centroid = centroids[small_cluster]

        # Identify all clusters except the small one
        other_clusters = [i for i in range(len(centroids)) if i != small_cluster]
        distances = cdist([small_cluster_centroid], centroids[other_clusters], metric='euclidean')
        nearest_cluster = other_clusters[np.argmin(distances)]

        # Reassign points from the small cluster to the nearest larger cluster
        labels[small_cluster_indices] = nearest_cluster

    return labels
