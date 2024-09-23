# %%
from sklearn.cluster import DBSCAN, HDBSCAN
import numpy as np
import geopandas as gpd 
import os
import pandas as pd 
from shapely import concave_hull, convex_hull, segmentize, minimum_rotated_rectangle
from shapely.ops import nearest_points
# ignore warnings
import warnings
warnings.filterwarnings('ignore')



dbscan_distance = 50
density_thresholds = [1, 3, 5, 10, 15]
concave_ratios = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99]


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


data_dir = r'D:\Projects\superparcels\data\urban'
output_dir = r'D:\Projects\superparcels\data\urban\outputs\dmatrix'
parcels = gpd.read_file(os.path.join(data_dir, 'sp_sample_08013_cluster_canidates.shp'))
utm = parcels.estimate_utm_crs().to_epsg()

parcels = parcels.to_crs(epsg=utm)  

unique_owners = parcels['OWNER'].unique()
print('Number of unique owners:', len(unique_owners))   

for density_threshold in density_thresholds:
    for concave_ratio in concave_ratios:
        print(f'Processing density threshold: {density_threshold} and concave ratio: {concave_ratio}')
        clustered_parcel_data = gpd.GeoDataFrame()
        single_parcel_data = gpd.GeoDataFrame()
        for owner in unique_owners:
            #print(f'OWNER: {owner}')
            owner_parcels = parcels[parcels['OWNER'] == owner]
            polygons = owner_parcels['geometry'].to_list()
            distance_matrix = compute_distance_matrix(polygons)


            dbscan = DBSCAN(eps=dbscan_distance, min_samples=2, metric='precomputed')
            clusters = dbscan.fit_predict(distance_matrix)
            owner_parcels['cluster'] = clusters 
            counts = owner_parcels['cluster'].value_counts()
            #print(f'Cluster Counts: {counts}')
            single_parcel_clusters = counts[counts == 1].index
            single_parcel_outliers = counts[counts.index == -1].index
            single_parcel_filter_ids = list(single_parcel_clusters) + list(single_parcel_outliers)
                
            single_parcel_filter = owner_parcels[owner_parcels['cluster'].isin(single_parcel_filter_ids)]
            single_parcel_data = pd.concat([single_parcel_data, single_parcel_filter], ignore_index=True)
            
            cluster_filter = owner_parcels[(~owner_parcels['cluster'].isin(single_parcel_clusters))&(owner_parcels['cluster'] != -1)]
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)
            #print('______________________________________________________________________________________')
            

            


        # create cluster ID
        clustered_parcel_data['cluster_ID'] = clustered_parcel_data['OWNER'] + '_' + clustered_parcel_data['cluster'].astype(str)
        single_parcel_data['cluster_ID'] = single_parcel_data['OWNER'] + '_' + single_parcel_data['cluster'].astype(str)


        parcel_dissolve = clustered_parcel_data.dissolve(by='cluster_ID').reset_index()


        super_parcels = parcel_dissolve.copy()


        # densify the super parcels
        super_parcels['geometry'] = super_parcels['geometry'].apply(lambda x: segmentize(x, density_threshold))


        super_parcels['geometry'] = super_parcels['geometry'].apply(lambda x: concave_hull(x, ratio=concave_ratio))


        try:
            for idx, parcel in super_parcels.iterrows():
                print(f'Cleaning super parcel: {parcel["cluster_ID"]}')
                parcel_geom = gpd.GeoDataFrame(geometry=[parcel.geometry])
                parcel_id = parcel['cluster_ID']    

                other_sp = super_parcels.loc[super_parcels['cluster_ID'] != parcel_id]
                other_single = single_parcel_data.loc[single_parcel_data['cluster_ID'] != parcel_id]
                other_sp_union = gpd.GeoDataFrame(geometry=[other_sp.unary_union])
                other_single_union = gpd.GeoDataFrame(geometry=[other_single.unary_union])
                other_union = pd.concat([other_sp_union, other_single_union], ignore_index=True)

                parcel_clip = (gpd.overlay(parcel_geom, other_union, how='difference')
                                .explode(ignore_index=True)
                                .reset_index(drop=True))
                                
                parcel_clip['cluster_ID'] = parcel_id
                parcel_clip['OWNER'] = parcel['OWNER']
                # drop correspnding row in super_parcels
                super_parcels = super_parcels[super_parcels['cluster_ID'] != parcel_id]
                # add parcel clip to super_parcels
                super_parcels = pd.concat([super_parcels, parcel_clip], ignore_index=True)
            
            super_parcels['cr'] = concave_ratio
            super_parcels['dt'] = density_threshold

            super_parcels = super_parcels.explode(ignore_index=True)
            super_parcels['sp_ID'] = super_parcels['cluster_ID'] + "_" + super_parcels.groupby('cluster_ID').cumcount().astype(str) 


            super_parcels[['sp_ID', 'cluster_ID', 'OWNER', 'geometry']].to_file(os.path.join(output_dir, f'sp_dbscan{dbscan_distance}-cr{concave_ratio}-dens{density_threshold}.shp'))
            print('______________________________________________________________________________________')
                
        except Exception as e:
            print('Error:', e)
            super_parcels[['cluster_ID', 'OWNER', 'geometry']].to_file(os.path.join(output_dir, f'sp_dbscan{dbscan_distance}-cr{concave_ratio}-dens{density_threshold}_FAILED.shp'))
            print('______________________________________________________________________________________')
            