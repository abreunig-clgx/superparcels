import numpy as np
from sklearn.cluster import DBSCAN, KMeans
from shapely.ops import nearest_points
import logging

logger = logging.getLogger(__name__)

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

def build_owner_clusters(df, min_samples, eps):
    """
    Builds clusters for a same-owner parcels within a region.
    DBSCAN is used to cluster parcels based on their distance
    using the calculated regional optimal distance. 
    """
    polygons = df.geometry.to_list()

    distance_matrix = compute_distance_matrix(polygons)

    if np.all(distance_matrix == 0): # if all adjacent parcels (i.e. zero-distances), then set distance to 1
        eps = 1

    if distance_matrix.shape[0] < 3: # only two parcels
        ##print('Only two parcels in region. No clustering performed.')
        dbscan = np.array([]) # no clustering
        return dbscan
    else:
        return build_dbscan_clusters(distance_matrix, min_samples, eps)

def build_dbscan_clusters(dmatrix, min_samples, eps):
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='precomputed')
    return dbscan.fit_predict(dmatrix)



""" Functions for KMeans clustering """
def build_place_regions(df, max_parcels_per_cluster):
    """
    Assigns each parcel to a region based on Kmeans clustering.
    Returns a list of region assignments.
    """

    # number of clustrs is proportional to the number of parcels
    if len(df) < max_parcels_per_cluster:
        nclusters = 1
    else:    
        nclusters = len(df) // max_parcels_per_cluster
    
    coords = build_coords(df)

    labels, centroids = build_kmeans_clusters(nclusters, coords)
    return labels, centroids

def build_kmeans_clusters(n_clusters, coords):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    labels = kmeans.fit_predict(coords)  
    centroids = kmeans.cluster_centers_

    return labels, centroids


def build_coords(df):
    return list(zip(df.centroid.x, df.centroid.y))

