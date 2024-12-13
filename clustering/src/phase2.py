
from shapely.ops import nearest_points
from sklearn.neighbors import NearestNeighbors
from scipy.ndimage import uniform_filter1d
import numpy as np
from math import ceil
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPolygon
from scipy.spatial import cKDTree
import warnings
warnings.filterwarnings('ignore')




def compute_regional_distance_matrix(df):
    """
    Computes the Distance Matrix for all polygons in each region (i.e. Place)
    Only the upper triangle of the matrix is computed as these are
    valid distances between polygons. The diagonal is removed as the
    distance between a polygon and itself is 0.
    """
    distance_matrix = df.geometry.apply(lambda g: df.distance(g)).values
    distances = distance_matrix[np.triu_indices_from(distance_matrix, k=1)]
    distances = distances[distances > 0]

    return distances

def compute_density(dmatrix, num_parcels):
    """
    Density = Number of Parcels / Range of the Distance Matrix
    """
    density = num_parcels / np.ptp(dmatrix) 
    return density

def compute_nneighbors(density, min_nneighbors):
    """
    Scales the number of N Neighbors based on the density of the Region. 
    Takes the minimum of the computed value or the total number of parcels - 1
    """
    n_neighbors = max(1, int(min_nneighbors / density))
    return n_neighbors

def compute_optimal_distance(dmatrix, n_neighbors, min_urban_distance, max_distance):
    """
    Computes the optimal distance for the DBSCAN algorithm.
        -Nearest Neighbors is used to fit the distance matrix and compute the distances for n_neighbors nearest neighbors.
        Dmatrix is reshaped to a 2d array to fit the Nearest Neighbors model.
        -The distances are sorted and smoothed using a uniform filter. This gives us a smooth curve of sorted sitances which helps make the elbow more accurate.
        -The difference between the smoothed distances is computed to find the elbow point.
        -The optimal distance is computed as the distance at the elbow point. This distance is then rounded up and constrained to be between the min_urban_distance and max_distance.
        
        Min_urban_distance is the minimum distance between two liekly neighbors.
        Max_distance is the maximum distance between two likely neighbors. This will mostley be set to 200 meters.
    """
    
    knn = NearestNeighbors(n_neighbors=n_neighbors).fit(dmatrix.reshape(-1, 1))
    knn_distances, _ = knn.kneighbors(dmatrix.reshape(-1, 1))
    sorted_distances = np.sort(knn_distances[:, -1])
    smooth_dist = uniform_filter1d(sorted_distances, size=10)
    difference = np.diff(smooth_dist)
    elbow_index = np.argmax(difference) + 1
    # take distance from KNN elbow --> must be greater than min_urban_distance and less than max_distance
    knn_optimal_distance = smooth_dist[elbow_index]
    optimal_distance = min(max(ceil(knn_optimal_distance), min_urban_distance), max_distance)
    
    return optimal_distance, knn_optimal_distance

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

def add_attributes(df, **kwargs):
    for key, value in kwargs.items():
        df[key] = value
    return df
