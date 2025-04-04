
import pandas as pd
import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')
import logging

from sp_geoprocessing.cluster import build_owner_clusters
from sp_geoprocessing.superparcels import (
    build_superparcels,
    hash_puids, 
    remove_overlap,
    remove_invalid_geoms
)
from sp_geoprocessing.utils import (
    add_attributes,
    remove_from_df,
    segregate_outliers,
    add_attributes
)

logger = logging.getLogger(__name__)
def build_sp_fixed(
    parcels, 
    fips,
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
    #class TqdmToLogger:
    #    def write(self, message):
    #        # Avoid logging empty messages (e.g., newlines)
    #        message = message.strip()
    #        if message:
    #            logger.info(message)
    #    def flush(self):
    #        pass
    parcels = parcels.reset_index(drop=True)
    parcels['puid'] = parcels.index
    # setup cProfiler
    #if qa:
    #    # enable cProfiler
    #    pass
    
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    unique_owners = parcels[key_field].unique()

    clustered_parcel_data = gpd.GeoDataFrame() # cluster data
    #single_parcel_data = gpd.GeoDataFrame() # non-clustered data
    logger.info(f'Building super parcels for {fips} and dt {distance_threshold}...')
   
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner] # ownder specific parcels
        
        # CLUSTERING
        clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=distance_threshold
            )

        if len(clusters) == 0: # EMPTY: NO CLUSTERS
            continue

        owner_parcels['cluster'] = clusters # clustert ID
        owner_parcels['cluster_area'] = owner_parcels['geometry'].area
        owner_parcels['cluster_area'] = owner_parcels['cluster_area'].astype(int)

        counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
        
        outlier_ids, clean_counts = segregate_outliers(counts, -1)

        cluster_filter = remove_from_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        )
        
    
        if len(cluster_filter) > 0:
            # calcualte total area
            total_area = cluster_filter.groupby('cluster')['cluster_area'].sum()

            # add attributes
            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(clean_counts),
                p_area=cluster_filter['cluster'].map(total_area),
            )
            cluster_filter = cluster_filter[[key_field, 'puid', 'cluster', 'pcount', 'p_area', 'geometry']]
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

    if len(clustered_parcel_data) == 0:
        return None # no clusters for input county candidate parcels

    # REFACTOR: cluster ID       
    clustered_parcel_data['cluster_ID'] = (
        clustered_parcel_data[key_field] + '_' +
        clustered_parcel_data['cluster'].astype(str)
    )

    # REFACTOR: SUPER PARCELS creation with area theshold option
    if area_threshold:
        # log message not implemented yet
        # run filter_area()
        pass
    
    # CLUSTER IDS PUIDS (eg. cluster0: [p1, p2, p3], cluster1: [p4, p5])
    cluster_puid_gb = clustered_parcel_data.groupby('cluster_ID')['puid'].apply(list).reset_index()
    
    super_parcels = build_superparcels(
        df=clustered_parcel_data,
        buffer=distance_threshold,
        dissolve_by='cluster_ID',
    )

    # CREATE HASED UNIQUE SP_ID
    super_parcels['sp_id'] = cluster_puid_gb['puid'].apply(hash_puids)

    # ADD OTHER ATTRIBUTES
    super_parcels = add_attributes(
        super_parcels,
        fips=fips,
        sp_area=super_parcels['geometry'].area,
        area_ratio=super_parcels['p_area'] / super_parcels['geometry'].area,
    )
    super_parcels['sp_area'] = super_parcels['sp_area'].astype(int)
    super_parcels['p_area'] = super_parcels['p_area'].astype(int)

    # REMOVE OVERLAPS
    super_parcels = remove_overlap(super_parcels)

    # REMOVE INVALID GEOMETRIES
    super_parcels = remove_invalid_geoms(super_parcels)
    # FINAL TABLE
    super_parcels = (
        super_parcels[['fips', 'sp_id', 'cluster_ID', key_field, 'pcount', 'area_ratio', 'p_area', 'sp_area', 'cbi', 'geometry']]
        .to_crs(epsg=4326)
    )

    logger.info(f'Finished building super parcels for {fips} and dt {distance_threshold}...')
    return super_parcels


def build_sp_multi(
    parcels, 
    fips,
    key_field='OWNER',
    distance_thresholds=[30,50,75,100], 
    sample_size=3,
    area_threshold=None,
    qa=False # if true, trigger cprofiler
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
    class TqdmToLogger:
        def write(self, message):
            # Avoid logging empty messages (e.g., newlines)
            message = message.strip()
            if message:
                logger.info(message)
        def flush(self):
            pass
    parcels = gpd.read_file(parcels)
    # setup cProfiler
    if qa:
        # enable cProfiler
        pass
    
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    unique_owners = parcels[key_field].unique()

    clustered_parcel_data = gpd.GeoDataFrame() # cluster data
    #single_parcel_data = gpd.GeoDataFrame() # non-clustered data
    logger.info(f'Building super parcels for {fips} and dt {distance_thresholds}...')
   
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner] # ownder specific parcels
        
        # REFACTOR: CLUSTERING
        clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=distance_thresholds
            )

        if len(clusters) == 0: # EMPTY: NO CLUSTERS
            #single_parcel_data = pd.concat([single_parcel_data, owner_parcels], ignore_index=True)  
            #single_parcel_data = add_attributes(
            #    single_parcel_data,
            #    #place_id=place_id,
            #    )
            continue

        owner_parcels['cluster'] = clusters # clustert ID
        owner_parcels['cluster_area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
        
        outlier_ids, clean_counts = segregate_outliers(counts, -1)

        #add_to_singles = locate_in_df(
        #    df=owner_parcels, 
        #    list_of_ids=outlier_ids, 
        #    field='cluster'
        #).drop(columns=['cluster', 'cluster_area'])

        #single_parcel_data = pd.concat([single_parcel_data, add_to_singles], ignore_index=True)

        #REFACTOR: WHAT FIELDS GO HERE??
        #if len(single_parcel_data) > 0:
        #    single_parcel_data = add_attributes(
        #        single_parcel_data,
        #        #place_id=place_id,
        #    )

        cluster_filter = remove_from_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        )
        
        # REFACTOR: ADD ATTRIBUTES??
        if len(cluster_filter) > 0:
            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(clean_counts)
            )
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

    if len(clustered_parcel_data) == 0:
        return None # no clusters for input county candidate parcels

    # REFACTOR: cluster ID       
    clustered_parcel_data['cluster_ID'] = (
        clustered_parcel_data[key_field] + '_' +
        clustered_parcel_data['cluster'].astype(str)
    )

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
    super_parcels = super_parcels[['sp_id', key_field, 'pcount', 'geometry']]

    logger.info(f'Finished building super parcels for {fips} and dt {distance_threshold}...')
    return super_parcels