from shapely.ops import nearest_points
from sklearn.neighbors import NearestNeighbors
from scipy.ndimage import uniform_filter1d
import numpy as np
from math import ceil
from sklearn.cluster import DBSCAN, KMeans
from shapely.geometry import MultiPolygon, MultiPoint
from scipy.spatial import cKDTree
from scipy.spatial.distance import cdist

from typing import List

""" Functions for KMeans clustering """
def build_place_regions(df, max_parcels_per_cluster):
    """
    Assigns each parcel to a region based on Kmeans clustering.
    Returns a list of region assignments.
    """

    # number of clustrs is proportional to the number of parcels
    nclusters = len(df) // max_parcels_per_cluster
    
    coords = build_coords(df)

    labels, centroids = build_kmeans_clusters(nclusters, coords)
    return labels, centroids

def build_kmeans_clusters(n_clusters, coords):
    kmeans = KMeans(n_clusters=n_clusters)
    labels = kmeans.fit_predict(coords)  
    centroids = kmeans.cluster_centers_

    return labels, centroids


def build_coords(df):
    return list(zip(df.centroid.x, df.centroid.y))

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




        

""" Functions for DBSCAN clustering """

def polygon_distance(polygon1, polygon2):
    # Calculate the minimum distance between two polygons
    point1, point2 = nearest_points(polygon1, polygon2)
    return point1.distance(point2)

def compute_distance_matrix(polygons):
    # Create a distance matrix between all polygons
    num_polygons = len(polygons)
    distance_matrix = np.zeros((num_polygons, num_polygons))
    
    for i in range(num_polygons):
        for j in range(i + 1, num_polygons):
            distance_matrix[i, j] = polygon_distance(polygons[i], polygons[j])
            distance_matrix[j, i] = distance_matrix[i, j]  # Symmetry
    
    return distance_matrix

def add_attributes(df, **kwargs):
    for key, value in kwargs.items():
        df[key] = value
    return df

def build_owner_clusters(df, min_samples, eps):
    """
    Builds clusters for a same-owner parcels within a region.
    DBSCAN is used to cluster parcels based on their distance
    using the calculated regional optimal distance. 
    """
    polygons = df.geometry.to_list()

    distance_matrix = compute_distance_matrix(polygons)

    if distance_matrix.shape[0] < 3: # only two parcels
        ##print('Only two parcels in region. No clustering performed.')
        dbscan = np.array([]) # no clustering
        return dbscan
    else:
        return build_dbscan_clusters(distance_matrix, min_samples, eps)

def build_dbscan_clusters(dmatrix, min_samples, eps):
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='precomputed')
    return dbscan.fit_predict(dmatrix)

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


""" Funcitons to Merge """
def merge_cross_region_clusters(df, max_merge_distance=4):
    # Step 1: Identify owners spanning multiple regions
    owner_region_count = df.groupby(['OWNER', 'place_id']).size().unstack(fill_value=0)
    multi_region_owners = owner_region_count[owner_region_count.sum(axis=1) > 1].index

    # Step 2: For each multi-region owner, check distances between clusters
    for owner in multi_region_owners:
        
        owner_df = df[df['OWNER'] == owner]
        
        # Extract centroids for each cluster of the owner
        centroids = owner_df.geometry.centroid
        coords = list(zip(centroids.x, centroids.y))

        # Build a KDTree for fast distance lookup
        tree = cKDTree(coords)

        # Find pairs of clusters within the max_merge_distance
        pairs = tree.query_pairs(max_merge_distance)
        
        # Merge clusters if they are close enough
        for i, j in pairs:
            cluster_i = owner_df.iloc[i]['cluster_ID']
            cluster_j = owner_df.iloc[j]['cluster_ID']

            # Update cluster_ID to merge them
            df.loc[df['cluster_ID'] == cluster_j, 'cluster_ID'] = cluster_i

    return df

   