


from phase2 import *
import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from shapely.geometry import MultiPoint
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
start = datetime.now()
max_parcels_per_cluster = 200 # maximum number of parcels per Kmeans cluster
kneighbors = 3 # number of nearest neighbors for optimal distance calculation
min_cluster_size = 3 # minimum number of parcels required to form a DBSCAN cluster
max_urban_distance = 120 # max. distance between two likely neighbors
rural_distance = 200 # distance between two parcels in a rural area
smoothing_window = 0.5 # 50% of the distance data is used for smoothing (i.e. 50% of the data is used for the window)
min_urban_distance = 15 # minimum distance between two parcels in an urban area

data_dir = r'D:\Projects\superparcels\data\phase2\la_ca'
print('Reading data...')
place_gdf = gpd.read_file(os.path.join(data_dir, 'tl_pl_los_angeles.shp'))[['PLACEFP', 'NAME', 'geometry']]
utm_crs = place_gdf.estimate_utm_crs().to_epsg()
place_gdf = place_gdf.to_crs(epsg=utm_crs)
#place_gdf = place_gdf.loc[place_gdf['PLACEFP'] == '41992']
parcels = gpd.read_file(os.path.join(data_dir, 'sp_sample_06037.shp'))
cc_parcels = gpd.read_file(os.path.join(data_dir, 'sp_sample_06037_cluster_candidates.shp'))

parcels = parcels.to_crs(epsg=utm_crs)
cc_parcels = cc_parcels.to_crs(epsg=utm_crs)

all_clustered_parcel_data = gpd.GeoDataFrame()
all_single_parcel_data = gpd.GeoDataFrame()
final_cluster_geodataframe = gpd.GeoDataFrame()
for place_id, place_data in place_gdf.iterrows():
    print('________________________________')
    #print(f'All Clustered Parcel Data: {all_clustered_parcel_data.shape}')
    #print(all_clustered_parcel_data.columns)
    place_id += 1
    place_name = place_data['NAME']
    place_area = place_data['geometry'].area
    #if place_name != 'Brea':
    #    continue

    print(f"Processing place: {place_name}")
    sub_parcels = parcels[parcels.within(place_data['geometry'])]
    if len(sub_parcels) == 0:
        continue
    
    regions, region_centroids = build_place_regions(sub_parcels, max_parcels_per_cluster)
    print(f"Number of regions: {len(set(regions))}")
    if len(set(regions)) == 1:
        merged_regions = regions
    else:
        merged_regions = merge_small_clusters(
            regions,
            region_centroids,
            kneighbors + 1
        )
    print(f"Number of merged regions: {len(set(merged_regions))}")
    
    sub_parcels['regions'] = merged_regions
    
        
    all_regional_parcels = gpd.GeoDataFrame()
    for region in tqdm(sub_parcels['regions'].unique(), desc='Regions', ncols=100):
        #print('________________________________')
        #print(f"Processing region {region}")
       
        clustered_parcel_data = gpd.GeoDataFrame()
        single_parcel_data = gpd.GeoDataFrame()
        
        regional_parcels = sub_parcels[sub_parcels['regions'] == region]
        region = regional_parcels['regions'].iloc[0]
        #print(f'Number of parcels in region {region}: {len(regional_parcels)}')
        # find cc_parcels that intersect with regional_parcels
        regional_cc_parcels = cc_parcels[cc_parcels.intersects(regional_parcels.unary_union)]

        if len(regional_cc_parcels) == 0:
            continue
        regional_cc_parcels['region'] = region
        regional_cc_parcels['place_id'] = place_id
        regional_cc_parcels['place_name'] = place_name
        regional_cc_parcels['place_area'] = place_area
        regional_cc_parcels['place_count'] = len(sub_parcels)
        #print(f"Number of cc_parcels in region {region}: {len(regional_cc_parcels)}")
        #print(f"Number of parcels in region {region}: {len(regional_parcels)}")

        region_coords = build_coords(regional_parcels)
        
        regional_singles = []
        if len(regional_parcels) == 1:
            regional_singles.append(regional_parcels)
            continue

        knn_optimal_distance = calculate_regional_knn_distance(
            coords=region_coords,
            kneighbors=kneighbors,
            smoothing_window=smoothing_window,
            min_distance=min_urban_distance,
            max_distance=max_urban_distance
        )
        smt_wndw = smoothing_window*len(regional_parcels)
        #print(f'Smoothing window Parcels: {smt_wndw}')
        #print(f'KNeighbors: {kneighbors}')
        #print(f"Optimal distance for Place {place_id}, Region {region}: {knn_optimal_distance}")
        
        
        # use the map funciton to apply values for subset of data
        regional_cc_parcels['knn_dist'] = knn_optimal_distance
        
        
        all_regional_parcels = pd.concat([all_regional_parcels, regional_cc_parcels], ignore_index=True)
        #print(all_regional_parcels.head())
        
        
        unique_owners = regional_cc_parcels['OWNER'].unique()
        for owner in unique_owners:
            #print(f"Processing owner {owner}")
            owner_parcels = regional_cc_parcels[regional_cc_parcels['OWNER'] == owner]
                     
            clusters = build_owner_clusters(
                owner_parcels,
                min_samples=min_cluster_size,
                eps=knn_optimal_distance
            )
            
            if len(clusters) == 0: # EMPTY: NO CLUSTERS
                #print(f'Owner {owner} has less than 3 parcels')
                single_parcel_data = pd.concat([single_parcel_data, owner_parcels], ignore_index=True)  
                single_parcel_data = add_attributes(
                    single_parcel_data,
                    place_id=place_id,
                    )
                continue

            owner_parcels['cluster'] = clusters # cluster ID
            owner_parcels['area'] = owner_parcels['geometry'].area
            counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts

            outlier_ids, clean_counts = segregate_outliers(counts, -1)

            add_to_singles = locate_in_df(owner_parcels, outlier_ids, 'cluster')
            add_to_singles = add_to_singles.drop(columns=['cluster', 'area'])
            single_parcel_data = pd.concat([single_parcel_data, add_to_singles], ignore_index=True)

            if len(single_parcel_data) > 0:
                single_parcel_data = add_attributes(
                    single_parcel_data,
                    place_id=place_id,
                )
            
            
            cluster_filter = remove_from_df(owner_parcels, outlier_ids, 'cluster')
            if len(cluster_filter) > 0:
                cluster_filter = add_attributes(
                    cluster_filter,
                    pcount=cluster_filter['cluster'].map(counts),
                    knn_dist=knn_optimal_distance,
                )
                clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)
           
        # create cluster ID
        if len(clustered_parcel_data) != 0:
            #print(f'Cluster Dataframe {region}: {clustered_parcel_data.shape}')
            cluster_string = generate_cluster_string([str(place_id), str(region)])
            
            clustered_parcel_data['cluster_ID'] = (
                clustered_parcel_data['OWNER'] + '_' +
                cluster_string + '_' + 
                clustered_parcel_data['cluster'].astype(str)
            )
            all_clustered_parcel_data = pd.concat([all_clustered_parcel_data, clustered_parcel_data], ignore_index=True)
        else:
            continue

        if len(single_parcel_data) != 0:
            cluster_string = generate_cluster_string([str(place_id), str(region), 'X'])

            single_parcel_data['cluster_ID'] = (
                single_parcel_data['OWNER'] + '_' +
                cluster_string
            )
            all_single_parcel_data = pd.concat([all_single_parcel_data, single_parcel_data], ignore_index=True)
        #print('________________________________')
                
        
    if len(all_clustered_parcel_data) == 0:
        continue

    for single_id, single_data in all_single_parcel_data.iterrows():
        owner = single_data['OWNER']
        same_owner_n = all_clustered_parcel_data[(all_clustered_parcel_data['OWNER'] == owner)]
        if same_owner_n.empty:
            continue
        same_owner_n['cross_dist'] = same_owner_n['geometry'].apply(lambda x: polygon_distance(x, single_data['geometry']))
        same_owner_nclusters = same_owner_n.loc[same_owner_n['cross_dist'] <= max_urban_distance]
        
        
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


    all_clustered_parcel_data_merged = merge_cross_region_clusters(
        all_clustered_parcel_data,
        max_merge_distance=max_urban_distance)


    #parcel_dissolve_merge = all_clustered_parcel_data_merged.dissolve(by='cluster_ID').reset_index()


    #parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(x['knn_dist']), axis=1)
    #parcel_dissolve_merge['geometry'] = parcel_dissolve_merge.apply(lambda x: x['geometry'].buffer(-x['knn_dist']), axis=1)

    all_clustered_parcel_data_merged = (all_clustered_parcel_data_merged
        [['FIPS', 
        'OWNER', 
        'place_id', 
        'place_name',
        'place_count',
        'place_area',
        'region', 
        'cluster_ID',
        'area', 
        'knn_dist', 
        'pcount', 
        'geometry']]
    )

    
    final_cluster_geodataframe = pd.concat([final_cluster_geodataframe, all_clustered_parcel_data_merged], ignore_index=True)
    # remove dataframes from memory
    all_clustered_parcel_data = gpd.GeoDataFrame()
    all_single_parcel_data = gpd.GeoDataFrame()
    parcel_dissolve_merge = None
    all_clustered_parcel_data_merged = None

place_processing_time = datetime.now() - start
print(f"Place processing time: {place_processing_time}")
rural_processing_time = datetime.now()
# RURAL CLUSTERING
all_rural_clustered_parcel_data = gpd.GeoDataFrame()
all_rural_single_parcel_data = gpd.GeoDataFrame()

# find parcels not within a place boundary
rural_parcels = cc_parcels[~cc_parcels.within(place_gdf.unary_union)]
unique_rural_owners = rural_parcels['OWNER'].unique()
rural_place_id = 99

unique_rural_owners = rural_parcels['OWNER'].unique()
rural_single_parcel_data = gpd.GeoDataFrame()
rural_clustered_parcel_data = gpd.GeoDataFrame()
for owner in tqdm(unique_rural_owners, desc='Rural Owners', ncols=100):
   
    owner_parcels = rural_parcels[rural_parcels['OWNER'] == owner]
                
    clusters = build_owner_clusters(
        owner_parcels,
        min_samples=min_cluster_size,
        eps=rural_distance
    )
    
    if len(clusters) == 0: # EMPTY: NO CLUSTERS
        rural_single_parcel_data = pd.concat([rural_single_parcel_data, owner_parcels], ignore_index=True)  
        rural_single_parcel_data = add_attributes(
            rural_single_parcel_data,
            place_id=place_id,
            )
        continue

    owner_parcels['cluster'] = clusters # cluster ID
    owner_parcels['area'] = owner_parcels['geometry'].area
    counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts

    outlier_ids, clean_counts = segregate_outliers(counts, -1)

    add_to_singles = locate_in_df(owner_parcels, outlier_ids, 'cluster')
    add_to_singles = add_to_singles.drop(columns=['cluster', 'area'])
    rural_single_parcel_data = pd.concat([rural_single_parcel_data, add_to_singles], ignore_index=True)

    if len(rural_single_parcel_data) > 0:
        rural_single_parcel_data = add_attributes(
            rural_single_parcel_data,
            place_id=rural_place_id,
        )
    
    
    cluster_filter = remove_from_df(owner_parcels, outlier_ids, 'cluster')
    if len(cluster_filter) > 0:
        cluster_filter = add_attributes(
            cluster_filter,
            pcount=cluster_filter['cluster'].map(counts),
            knn_dist=rural_distance,
            place_id=rural_place_id,
            region=rural_place_id
        )
        rural_clustered_parcel_data = pd.concat([rural_clustered_parcel_data, cluster_filter], ignore_index=True)
    
# create cluster ID
if len(rural_clustered_parcel_data) != 0:
    cluster_string = generate_cluster_string([str(rural_place_id), str(region)])
    
    rural_clustered_parcel_data['cluster_ID'] = (
        rural_clustered_parcel_data['OWNER'] + '_' +
        cluster_string + '_' + 
        rural_clustered_parcel_data['cluster'].astype(str)
    )

if len(rural_single_parcel_data) != 0:
    cluster_string = generate_cluster_string([str(rural_place_id), str(region), 'X'])

    rural_single_parcel_data['cluster_ID'] = (
        rural_single_parcel_data['OWNER'] + '_' +
        cluster_string
    )
print('________________________________')
        
all_rural_clustered_parcel_data = pd.concat([all_rural_clustered_parcel_data, rural_clustered_parcel_data], ignore_index=True)
all_rural_single_parcel_data = pd.concat([all_rural_single_parcel_data, rural_single_parcel_data], ignore_index=True)

final_cluster_geodataframe_wrural = pd.concat([final_cluster_geodataframe, all_rural_clustered_parcel_data], ignore_index=True)
all_single_parcel_data_wrural = pd.concat([all_single_parcel_data, all_rural_single_parcel_data], ignore_index=True)

final_cluster_geodataframe_merged_wrural = merge_cross_region_clusters(
    final_cluster_geodataframe_wrural, 
    max_merge_distance=rural_distance
)

final_cluster_geodataframe_dissolved = final_cluster_geodataframe_merged_wrural.dissolve(by='cluster_ID').reset_index()

final_cluster_geodataframe_dissolved['geometry'] = final_cluster_geodataframe_dissolved.apply(lambda x: x['geometry'].buffer(x['knn_dist']), axis=1)
final_cluster_geodataframe_dissolved['geometry'] = final_cluster_geodataframe_dissolved.apply(lambda x: x['geometry'].buffer(-x['knn_dist']), axis=1)

final_cluster_geodataframe_dissolved = (final_cluster_geodataframe_dissolved
    [['FIPS', 
    'OWNER', 
    'place_id', 
    'place_name',
    'place_count',
    'place_area',
    'region', 
    'cluster_ID',
    'area', 
    'knn_dist', 
    'pcount', 
    'geometry']]
    )


final_cluster_geodataframe_dissolved.to_file(
    os.path.join(
        data_dir, 
        f'LA_SuperParcels_All_Distribution_v2.shp'))

print(f'Rural Processing Time: {datetime.now() - rural_processing_time}')
print('________________________________')

