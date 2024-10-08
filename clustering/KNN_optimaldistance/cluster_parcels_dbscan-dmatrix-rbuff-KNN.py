
from sklearn.neighbors import NearestNeighbors
from scipy.ndimage import uniform_filter1d
from sklearn.cluster import DBSCAN
import numpy as np
import geopandas as gpd 
import os
import sys
import glob
import pandas as pd 
from shapely.ops import nearest_points
from tqdm import tqdm
# ignore warnings
import warnings
warnings.filterwarnings('ignore')

sample_size = 3
area_threshold = 300_000 # sq. meters

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


data_dir = r'D:\Projects\superparcels\data\Rural'

for fi in glob.glob(os.path.join(data_dir, '*\*canidates.shp')):
    fips = os.path.basename(fi).split('_')[2]
    subdir = os.path.dirname(fi)
    
    print(f'Processing {os.path.basename(subdir)}: {fips}...')
   
  
    parcels = gpd.read_file(fi)


    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    unique_owners = parcels['OWNER'].unique()

    clustered_parcel_data = gpd.GeoDataFrame()
    single_parcel_data = gpd.GeoDataFrame()
    cluster_counts_dict = {}
    for owner in tqdm(unique_owners, desc=f'{fips} Owners: ', ncols=100):
        
        owner_parcels = parcels[parcels['OWNER'] == owner]
        polygons = owner_parcels['geometry'].to_list()
        distance_matrix = compute_distance_matrix(polygons)
        
        if distance_matrix.shape[0] < 3: # only two parcels
            continue
            
        if np.all(distance_matrix == 0): # get distance greater than 0 but minimum distance
            min_valid_distance = 3
            optimal_distance = 3
        else:
            min_valid_distance = np.round(np.min(distance_matrix[distance_matrix > 0]))

            # Assuming distance_matrix is the precomputed distance matrix
            neighbors = NearestNeighbors(n_neighbors=sample_size, metric='precomputed')
            neighbors_fit = neighbors.fit(distance_matrix)

            try:
                distances, indices = neighbors_fit.kneighbors(distance_matrix)
            except ValueError:
                print(f'Error: Not enough samples in {owner}')
                print(distance_matrix)
                sys.exit()

            # Sort distances to the k-th nearest neighbor
            sorted_distances = np.sort(distances[:, sample_size-1])
            smooth_dist = uniform_filter1d(sorted_distances, size=10)
            difference = np.diff(smooth_dist)
            elbow_index = np.argmax(difference) + 1

            optimal_distance = np.round(sorted_distances[elbow_index])
            if optimal_distance == 0: # adjacent parcels
                optimal_distance = 1

            distance_cap = 200 # maximum distance
            if optimal_distance > distance_cap:
                optimal_distance = distance_cap
        
        dbscan = DBSCAN(eps=optimal_distance, min_samples=sample_size, metric='precomputed')

        clusters = dbscan.fit_predict(distance_matrix)
        owner_parcels['cluster'] = clusters 
        owner_parcels['area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts()
        
        
        outliers = counts[counts.index == -1].index
        single_parcel_filter_ids = set(list(outliers))
            
        single_parcel_filter = owner_parcels[owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        single_parcel_data = pd.concat([single_parcel_data, single_parcel_filter], ignore_index=True)
        
        cluster_filter = owner_parcels[~owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        if len(cluster_filter) > 0:
            cluster_counts_dict[owner] = sum(counts.to_list())
            cluster_filter['buff_dist'] = optimal_distance
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)
      

    # create cluster ID
    if len(clustered_parcel_data) == 0:
        continue

    print(f'Cluster Dataframe {fips}: {clustered_parcel_data.shape}')
    clustered_parcel_data['cluster_ID'] = clustered_parcel_data['OWNER'] + '_' + clustered_parcel_data['cluster'].astype(str)
    single_parcel_data['cluster_ID'] = single_parcel_data['OWNER'] + '_' + single_parcel_data['cluster'].astype(str)
    clustered_parcel_data['pcount'] = clustered_parcel_data['OWNER'].map(cluster_counts_dict)

    parcel_dissolve = clustered_parcel_data.dissolve(by='cluster_ID').reset_index()
    # Define a function to buffer the geometry by its buff_dist
    def buffer_geometry(geometry, buff_dist):
        return geometry.buffer(buff_dist)

    # Use map to apply the buffer_geometry function to each geometry and buff_dist
    parcel_dissolve['geometry'] = list(map(buffer_geometry, parcel_dissolve['geometry'], parcel_dissolve['buff_dist']))
    parcel_dissolve['geometry'] = list(map(buffer_geometry, parcel_dissolve['geometry'], -parcel_dissolve['buff_dist']))
    parcel_dissolve = parcel_dissolve.explode(ignore_index=True)
    parcel_dissolve['sp_id'] = parcel_dissolve['cluster_ID'] + "_" + parcel_dissolve.groupby('cluster_ID').cumcount().astype(str) 

    parcel_dissolve['area'] = parcel_dissolve['geometry'].area
    #mean_area = parcel_dissolve.groupby('sp_id')['area'].mean()
    super_parcel_ids = parcel_dissolve[parcel_dissolve['area'] > area_threshold].index

    super_parcels = parcel_dissolve[parcel_dissolve.index.isin(super_parcel_ids)]
    super_parcels = super_parcels.sort_values(by='area', ascending=False)
    super_parcels['rank'] = super_parcels['area'].rank(ascending=False)
    
    def num_2_short_form(number):
        if number >= 1_000_000_000:
            return f'{number/1_000_000_000:.1f}B'
        elif number >= 1_000_000:
            return f'{number/1_000_000:.1f}M'
        elif number >= 1_000:
            return f'{number/1_000:.1f}k'
        else:
            return str(number)

    super_parcels['sq_meters'] = super_parcels['area'].apply(num_2_short_form)

    super_parcels = super_parcels[['cluster_ID', 'OWNER', 'area', 'sq_meters', 'rank', 'pcount', 'buff_dist', 'geometry']]

    if len(super_parcels) == 0:
        continue

    if len(single_parcel_data) > 0:
        single_parcel_data.to_file(os.path.join(subdir, f'singles_{fips}_KNN-dbscan-{sample_size}_area{area_threshold}_rbuff.shp'))


    super_parcels.to_file(os.path.join(subdir, f'sp_{fips}_KNN-dbscan-{sample_size}_area{area_threshold}_rbuff.shp'))
    print('_________________________________________________________')
    print('_________________________________________________________')
    





