
import os
import sys
import glob
import pandas as pd
from phase2 import *
from tqdm import tqdm
# ignore warnings
import warnings
warnings.filterwarnings('ignore')


# PARAMS
#sample_size = 3
#area_threshold = 300_000
#data_dir = r'D:\Projects\superparcels\data\Phase2\la_ca'
# candidate county shapefile

# OUTSIDE CONFIG IN MAIN
  #fips = os.path.basename(fi).split('_')[2]
   # subdir = os.path.dirname(fi)

    ##print(f'Processing {os.path.basename(subdir)}: {fips}...')

#parcels = gpd.read_file(fi)


#for fi in glob.glob(os.path.join(data_dir, '*candidates.shp')):

# AT END FOR WRITING
#if len(single_parcel_data) > 0:
#        single_parcel_data.to_file(os.path.join(subdir, f'singles_{fips}_dbscan{dbscan_distance}-{sample_size}_rbuff.shp'))#


#    super_parcels.to_file(os.path.join(subdir, f'sp_{fips}_dbscan{dbscan_distance}-{sample_size}_rbuff.shp'))
    #print('_________________________________________________________')
    #print('_________________________________________________________')
    

  
def build_sp_fixed(
    parcels, 
    key_field='OWNER',
    distance_threshold=200, 
    sample_size=3,
    area_threshold=None,
    ):
    """
    Executes the clustering and super parcel creation process.
    Uses a fixed epsilon value for DBSCAN clustering.

    Args:
    parcels (GeoDataFrame): Candidate parcels for super parcel creation.
    key_field (str): Field to use for clustering.
    distance_threshold (int): Distance threshold for DBSCAN clustering.
    sample_size (int): Minimum number of samples for DBSCAN clustering.
    area_threshold (int): Minimum area threshold for super parcel creation.
    """

    
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    unique_owners = parcels['OWNER'].unique()

    clustered_parcel_data = gpd.GeoDataFrame() # cluster data
    single_parcel_data = gpd.GeoDataFrame() # non-clustered data
    for owner in tqdm(unique_owners, desc=f'{fips} Owners: ', ncols=100):
        owner_parcels = parcels[parcels['OWNER'] == owner] # ownder specific parcels
        
        # REFACTOR: CLUSTERING
        clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=distance_threshold
            )

        if len(clusters) == 0: # EMPTY: NO CLUSTERS
            single_parcel_data = pd.concat([single_parcel_data, owner_parcels], ignore_index=True)  
            single_parcel_data = add_attributes(
                single_parcel_data,
                place_id=place_id,
                )
            continue

        owner_parcels['cluster'] = clusters # clustert ID
        owner_parcels['cluster_area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
        
        outlier_ids, clean_counts = segregate_outliers(counts, -1)

        add_to_singles = locate_in_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        ).drop(columns=['cluster', 'area'])

        single_parcel_data = pd.concat([single_parcel_data, add_to_singles], ignore_index=True)

        #REFACTOR: WHAT FIELDS GO HERE??
        if len(single_parcel_data) > 0:
            single_parcel_data = add_attributes(
                single_parcel_data,
                #place_id=place_id,
            )

        cluster_filter = remove_from_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        )
        
        # REFACTOR: ADD ATTRIBUTES??
        if len(cluster_filter) > 0:
            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(counts)
            )
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

    if len(clustered_parcel_data) == 0:
        return None # no clusters for input county candidate parcels

    # REFACTOR: cluster ID       
    clustered_parcel_data['cluster_ID'] = (
        clustered_parcel_data['OWNER'] + '_' +
        clustered_parcel_data['cluster'].astype(str)
    )

    # build cluster specific IDs
    clustered_parcel_data['cluster_ID'] = clustered_parcel_data['OWNER'] + '_' + clustered_parcel_data['cluster'].astype(str)
    single_parcel_data['cluster_ID'] = single_parcel_data['OWNER'] + '_' + single_parcel_data['cluster'].astype(str)

    # REFACTOR: SUPER PARCELS creation with area theshold option
    if area_threshold:
        # log message not implemented yet
        # run filter_area()
        pass

    super_parcels = build_superparcels(
        df=clustered_parcel_data,
        buffer=distance_threshold,
        dissolve_by='cluster_ID',
    )

    # super parcel ID eg. owner + cluster ID + ID of each unique super parcel
    super_parcels['sp_id'] = super_parcels['cluster_ID'] + "_" + super_parcels.groupby('cluster_ID').cumcount().astype(str) 
    super_parcels = super_parcels[['sp_id', 'OWNER', 'pcount', 'geometry']]

    if return_singles:
        return super_parcels, single_parcel_data
    else:
        return super_parcels
    




