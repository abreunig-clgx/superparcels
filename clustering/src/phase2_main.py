


from phase2 import *
import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
sample_size = 3 # clusters must have at least 3 parcels
min_urban_distance = 2 # min. distance between two likely neighbors
max_distance = 5 # max. distance between two likely neighbors

data_dir = r'D:\Projects\superparcels\data\phase2'
print('Reading data...')
place_gdf = gpd.read_file(os.path.join(data_dir, 'alameda_test_place.shp'))[['PLACEFP', 'geometry']]
utm_crs = place_gdf.estimate_utm_crs().to_epsg()
place_gdf = place_gdf.to_crs(epsg=utm_crs)
place_gdf = place_gdf.loc[place_gdf['PLACEFP'] == '41992']
parcels = gpd.read_file(os.path.join(data_dir, 'sp_sample_06001_cluster_canidates.shp'))
parcels = parcels.to_crs(epsg=utm_crs)


all_clustered_parcel_data = gpd.GeoDataFrame()
all_single_parcel_data = gpd.GeoDataFrame()
for place_id, place_data in place_gdf.iterrows():
    place_id += 1
    print(f"Processing place {place_id}")
    sub_parcels = parcels[parcels.intersects(place_data['geometry'])]
    print('Computing distance matrix...')
    distances = compute_regional_distance_matrix(sub_parcels)
    print('Computing density...')
    density = compute_density(distances, len(sub_parcels))
    print('Computing n_neighbors...')
    min_nneighbors = max(0.1 * len(sub_parcels), 3) # 10% of the total number of parcels
    n_neighbors = compute_nneighbors(density, min_nneighbors)

    optimal_distance, knn_optimal_distance = compute_optimal_distance(distances, n_neighbors, min_urban_distance, max_distance)
    print(f'Density for Place {place_id}: {density}')
    print(f'Min N Neighbors for Place {place_id}: {min_nneighbors}')
    print(f'N Neighbors for Place {place_id}: {n_neighbors}')
    print(f'KNN distance for Place {place_id}: {knn_optimal_distance}')
    print(f"Optimal distance for Place {place_id}: {optimal_distance}")
    print('________________________________________________________')
    unique_owners = sub_parcels['OWNER'].unique()
    clustered_parcel_data = gpd.GeoDataFrame()
    single_parcel_data = gpd.GeoDataFrame()

    for owner in tqdm(unique_owners, desc=f'Owners: ', ncols=100):
        owner_parcels = sub_parcels[sub_parcels['OWNER'] == owner]
        #print(f"Owner {owner} has {len(owner_parcels)} parcels")
        polygons = owner_parcels['geometry'].to_list()
        distance_matrix = compute_distance_matrix(polygons)
        if distance_matrix.shape[0] < 3: # only two parcels
            continue

        dbscan = DBSCAN(eps=optimal_distance, min_samples=sample_size, metric='precomputed')
        clusters = dbscan.fit_predict(distance_matrix)

        owner_parcels['cluster'] = clusters # clustert ID
        owner_parcels['area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
        
        outliers = counts[counts.index == -1].index # outliers always identified as -1
        counts = counts[counts.index != -1] # drop outliers

        single_parcel_filter_ids = set(list(outliers)) # not apart of any cluster
            
        single_parcel_filter = owner_parcels[owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        single_parcel_data = pd.concat([single_parcel_data, single_parcel_filter], ignore_index=True)
        
        cluster_filter = owner_parcels[~owner_parcels['cluster'].isin(single_parcel_filter_ids)]
        if len(cluster_filter) > 0:
            cluster_filter['pcount'] = cluster_filter['cluster'].map(counts) # add parcel count to filter dataframe
            cluster_filter['knn_dst'] = knn_optimal_distance
            cluster_filter['opt_dst'] = optimal_distance # dbscan distane is euivalent to the required buffer distance
            cluster_filter['place_id'] = place_id
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

    # create cluster ID
    if len(clustered_parcel_data) != 0:
        clustered_parcel_data['cluster_ID'] = clustered_parcel_data['OWNER'] + '_' + str(place_id) + '_' + clustered_parcel_data['cluster'].astype(str)
        all_clustered_parcel_data = pd.concat([all_clustered_parcel_data, clustered_parcel_data], ignore_index=True)
    all_single_parcel_data = pd.concat([all_single_parcel_data, single_parcel_data], ignore_index=True)

        

    

    print('________________________________________________________')
    

parcel_dissolve = all_clustered_parcel_data.dissolve(by='cluster_ID').reset_index()
parcel_dissolve


parcel_dissolve['geometry'] = parcel_dissolve.apply(lambda x: x['geometry'].buffer(x['opt_dst']), axis=1)
parcel_dissolve['geometry'] = parcel_dissolve.apply(lambda x: x['geometry'].buffer(-x['opt_dst']), axis=1)
parcel_dissolve = parcel_dissolve.explode(index_parts=True)

for single_id, single_data in all_single_parcel_data.iterrows():
    owner = single_data['OWNER']
    same_owner_n = all_clustered_parcel_data[(all_clustered_parcel_data['OWNER'] == owner)]
    if same_owner_n.empty:
        continue
    same_owner_n['cross_dist'] = same_owner_n['geometry'].apply(lambda x: polygon_distance(x, single_data['geometry']))
    same_owner_nclusters = same_owner_n.loc[same_owner_n['cross_dist'] <= 3]
    
    
    if same_owner_nclusters.empty:
        continue

    same_owner_ncluster = same_owner_nclusters.loc[same_owner_nclusters['cross_dist'].idxmin(), 'cluster_ID']
   
    add_single = all_single_parcel_data[all_single_parcel_data.index == single_id][['OWNER', 'geometry']]
    merge_n = all_clustered_parcel_data[all_clustered_parcel_data['cluster_ID'] == same_owner_ncluster]
    all_clustered_parcel_data = all_clustered_parcel_data[all_clustered_parcel_data['cluster_ID'] != same_owner_ncluster]

    same_owner_merge = pd.concat([same_owner_n, add_single], ignore_index=True).drop(columns=['cross_dist'])
    new_row_index = same_owner_merge.index[-1]
    source_row_index = same_owner_merge.index[0]
    for col in same_owner_merge.columns:
        if col != 'geometry':  # Skip the geometry field
            same_owner_merge.at[new_row_index, col] = same_owner_merge.at[source_row_index, col]

    all_clustered_parcel_data = pd.concat([all_clustered_parcel_data, same_owner_merge], ignore_index=True)


all_clustered_parcel_data_merged = merge_cross_region_clusters(all_clustered_parcel_data)


parcel_dissolve_merge = all_clustered_parcel_data_merged.dissolve(by='cluster_ID').reset_index()
parcel_dissolve_merge


parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(x['opt_dst']), axis=1)
parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(-x['opt_dst']), axis=1)

