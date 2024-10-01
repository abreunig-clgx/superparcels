
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
area_threshold = 300_000

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


data_dir = r'D:\Projects\superparcels\data\Urban'

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
    for owner in tqdm(unique_owners, desc=f'{fips} Owners: ', ncols=100):
        
        dbscan_distance = 200
        owner_parcels = parcels[parcels['OWNER'] == owner]
        polygons = owner_parcels['geometry'].to_list()
        distance_matrix = compute_distance_matrix(polygons)
        
        if distance_matrix.shape[0] < 3: # only two parcels
            continue

        if np.all(distance_matrix == 0): # get distance greater than 0 but minimum distance
            dbscan_distance = 3
        
        dbscan = DBSCAN(eps=dbscan_distance, min_samples=sample_size, metric='precomputed')
        clusters = dbscan.fit_predict(distance_matrix)
        owner_parcels['cluster'] = clusters 
        owner_parcels['area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts()
        
        outliers = counts[counts.index == -1].index
        # drop outliers
        counts = counts[counts.index != -1]
        single_parcel_filter_ids = set(list(outliers))
            
        single_parcel_filter = owner_parcels[owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        single_parcel_data = pd.concat([single_parcel_data, single_parcel_filter], ignore_index=True)
        
        cluster_filter = owner_parcels[~owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        if len(cluster_filter) > 0:
            cluster_filter['pcount'] = cluster_filter['cluster'].map(counts)
            cluster_filter['buff_dist'] = dbscan_distance
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)
      

    # create cluster ID
    # create cluster ID
    if len(clustered_parcel_data) == 0:
        continue

    print(f'Cluster Dataframe {fips}: {clustered_parcel_data.shape}')

    clustered_parcel_data['cluster_ID'] = clustered_parcel_data['OWNER'] + '_' + clustered_parcel_data['cluster'].astype(str)
    single_parcel_data['cluster_ID'] = single_parcel_data['OWNER'] + '_' + single_parcel_data['cluster'].astype(str)

    parcel_dissolve = clustered_parcel_data.dissolve(by='cluster_ID').reset_index()
    parcel_dissolve['area'] = parcel_dissolve['geometry'].area # total area of the cluster

    mean_area = parcel_dissolve.groupby('cluster_ID')['area'].mean()
    super_parcel_ids = mean_area[mean_area > area_threshold].index

    super_parcels = parcel_dissolve[parcel_dissolve['cluster_ID'].isin(super_parcel_ids)]
    if len(super_parcels) == 0:
        print(f'No super parcels found for {fips}')
        print(super_parcels['area'].sort_values(ascending=False))
        continue

    super_parcels['geometry'] = super_parcels['geometry'].buffer(dbscan_distance)
    super_parcels['geometry'] = super_parcels['geometry'].buffer(-dbscan_distance)
    super_parcels = super_parcels.explode(ignore_index=True)

    super_parcels['sp_id'] = super_parcels['cluster_ID'] + "_" + super_parcels.groupby('cluster_ID').cumcount().astype(str) 
    super_parcels['rank'] = super_parcels['area'].rank(ascending=False)
    super_parcels = super_parcels.sort_values(by='rank', ascending=True)
    super_parcels = super_parcels.reset_index(drop=True)

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
    super_parcels = super_parcels[['sp_id', 'OWNER', 'area', 'sq_meters', 'rank', 'pcount', 'buff_dist', 'geometry']]

    if len(single_parcel_data) > 0:
        single_parcel_data.to_file(os.path.join(subdir, f'singles_{fips}_dbscan{dbscan_distance}-{sample_size}_area{area_threshold}_rbuff.shp'))


    

    super_parcels.to_file(os.path.join(subdir, f'sp_{fips}_dbscan{dbscan_distance}-{sample_size}_area{area_threshold}_rbuff.shp'))
    print('_________________________________________________________')
    print('_________________________________________________________')
    





