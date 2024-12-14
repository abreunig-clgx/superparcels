


from phase2 import *
from sklearn.cluster import OPTICS, KMeans
import os
import sys
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from shapely.geometry import MultiPoint
cluster_method = 'dbscan' # cluster method
max_parcels_per_cluster = 500 # maximum number of parcels per cluster
min_samples = 5 # minimum number of samples required to form a cluster
sample_size = 10 # clusters must have at least 3 parcels
min_cluster_size = 50 # minimum number of parcels required to form a cluster
min_urban_distance = 100 # min. distance between two likely neighbors
max_distance = 200 # max. distance between two likely neighbors

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
    
    print(f'DBSCAN Cluster for Place {place_id}...')

    # number of clustrs is proportional to the number of parcels
    nclusters = len(sub_parcels) // max_parcels_per_cluster
    print(f'Number of clusters for Kmeans: {nclusters}')
    kmeans =  KMeans(n_clusters=nclusters)
    sub_parcel_centroids = sub_parcels.centroid
    sub_parcel_coords = list(zip(sub_parcel_centroids.x, sub_parcel_centroids.y))
    regions = kmeans.fit_predict(sub_parcel_coords)
    sub_parcels['regions'] = regions
    #print('writing regions to file...') 
    #sub_parcels.to_file(
    #    os.path.join(
    #        data_dir, 
    #        f'place_{place_id}_regions_kmeans_{max_parcels_per_cluster}.shp'))
    #print('________________________________________________________')
    #sys.exit()
    # sorted
    region_list = sorted(sub_parcels['regions'].unique())
    print(f'Number of regions for Place {place_id}: {len(region_list)}')
    for region in region_list:
        print('________________________________')
        print(f"Processing region {region}")
        clustered_parcel_data = gpd.GeoDataFrame()
        single_parcel_data = gpd.GeoDataFrame()
        
        regional_parcels = sub_parcels[sub_parcels['regions'] == region]
        regional_parcels_centroid = regional_parcels.centroid
        regional_parcels_coords = list(zip(regional_parcels_centroid.x, regional_parcels_centroid.y))
        parcel_count = len(regional_parcels)
        regional_area = MultiPoint(regional_parcels_coords).convex_hull.area 
        print(f"Region {region} has {parcel_count} parcels")
        print(f"Region {region} area: {regional_area}")
        density = parcel_count / regional_area

        print('Computing KDTree...')
        dtree = cKDTree(regional_parcels_coords)
        print('Computing density...')
        
        n_neighbors = compute_nneighbors(density)

        print('Computing optimal distance...')
        knn_distances, _ = dtree.query(regional_parcels_coords, k=n_neighbors)
        sorted_distances = np.sort(knn_distances[:, -1])
        smooth_dist = uniform_filter1d(sorted_distances, size=10)
        difference = np.diff(smooth_dist)
        elbow_index = np.argmax(difference) + 1
        # take distance from KNN elbow --> must be greater than min_urban_distance and less than max_distance
        knn_optimal_distance = smooth_dist[elbow_index]
        if knn_optimal_distance > 30:
            optimal_distance = 30
        #optimal_distance = min(max(ceil(knn_optimal_distance), min_urban_distance), max_distance)

        print(f'Density for Place {place_id}, Region {region}: {density}')
        #print(f'Min N Neighbors for Place {place_id}, Region {region}: {min_nneighbors}')
        print(f'N Neighbors for Place {place_id}, Region {region}: {n_neighbors}')
        print(f'KNN distance for Place {place_id}, Region {region}: {knn_optimal_distance}')
        print(f"Optimal distance for Place {place_id}, Region {region}: {optimal_distance}")
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
                
                single_parcel_data = pd.concat([single_parcel_data, owner_parcels], ignore_index=True)  
                single_parcel_data = add_attributes(
                    single_parcel_data,
                    place_id=place_id,
                    )
                continue

            dbscan = DBSCAN(eps=optimal_distance, min_samples=sample_size, metric='precomputed')
            clusters = dbscan.fit_predict(distance_matrix)

            owner_parcels['cluster'] = clusters # clustert ID
            owner_parcels['area'] = owner_parcels['geometry'].area
            counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
            
            outliers = counts[counts.index == -1].index # outliers always identified as -1
            counts = counts[counts.index != -1] # drop outliers

            single_parcel_filter_ids = set(list(outliers)) # not apart of any cluster
                
            single_parcel_filter = owner_parcels[owner_parcels['cluster'].isin(single_parcel_filter_ids)].drop(columns=['cluster', 'area'])
            single_parcel_data = pd.concat([single_parcel_data, single_parcel_filter], ignore_index=True)
            
            if len(single_parcel_filter) > 0:
                single_parcel_filter = add_attributes(
                    single_parcel_filter,
                    place_id=place_id,
                )

            cluster_filter = owner_parcels[~owner_parcels['cluster'].isin(single_parcel_filter_ids)]
            if len(cluster_filter) > 0:
                cluster_filter = add_attributes(
                    cluster_filter,
                    pcount=cluster_filter['cluster'].map(counts),
                    knn_dst=knn_optimal_distance,
                    opt_dst=optimal_distance,
                    place_id=place_id
                )
                clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

        if len(clustered_parcel_data) != 0:
            clustered_parcel_data['cluster_ID'] = (
                clustered_parcel_data['OWNER'] + 
                '_' + 
                str(place_id) +
                '-' +
                str(region) +
                '-' + 
                clustered_parcel_data['cluster'].astype(str)
            )

        if len(single_parcel_data) != 0:
            #print(f'single parcel data: region:{region}, owner:{owner}')
            single_parcel_data['cluster_ID'] = (
                single_parcel_data['OWNER'] + 
                '_' + 
                str(place_id) +
                '-' +
                str(region) +
                '-' +
                'X'
            )
        print('________________________________')
                
        all_clustered_parcel_data = pd.concat([all_clustered_parcel_data, clustered_parcel_data], ignore_index=True)
        all_single_parcel_data = pd.concat([all_single_parcel_data, single_parcel_data], ignore_index=True)

print('________________________________________________________')

parcel_dissolve = all_clustered_parcel_data.dissolve(by='cluster_ID').reset_index()

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

    same_owner_merge = pd.concat([merge_n, add_single], ignore_index=True)
    new_row_index = same_owner_merge.index[-1]
    source_row_index = same_owner_merge.index[0]
    for col in same_owner_merge.columns:
        if col != 'geometry':  # Skip the geometry field
            same_owner_merge.at[new_row_index, col] = same_owner_merge.at[source_row_index, col]

    all_clustered_parcel_data = pd.concat([all_clustered_parcel_data, same_owner_merge], ignore_index=True)


all_clustered_parcel_data_merged = merge_cross_region_clusters(all_clustered_parcel_data)


parcel_dissolve_merge = all_clustered_parcel_data_merged.dissolve(by='cluster_ID').reset_index()


parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(x['opt_dst']), axis=1)
parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(-x['opt_dst']), axis=1)

parcel_dissolve_merge.to_file(
    os.path.join(
        data_dir, 
        f'place_{place_id}_superparcels.shp'))

