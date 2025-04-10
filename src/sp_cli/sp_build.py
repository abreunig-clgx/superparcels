
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
    logger.info('Removing overlaps...')
    super_parcels = remove_overlap(super_parcels)

    # REMOVE INVALID GEOMETRIES
    logger.info(f'Shape before removing invalid geometries: {super_parcels.shape}')
    logger.info('Removing invalid geometries...')
    super_parcels, invalid_geoms = remove_invalid_geoms(super_parcels)
    logger.info(f'Shape after removing invalid geometries: {super_parcels.shape}')
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
    distance_thresholds=[200], 
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
    logger.info('Building super parcels with multi eps...')
    parcels = parcels.reset_index(drop=True)
    parcels['puid'] = parcels.index
    
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  
    logger.info(f'Using UTM CRS {utm}...')
    unique_owners = parcels[key_field].unique()
    logger.info(f'Unique owners: {len(unique_owners)}')
    clustered_parcel_data = gpd.GeoDataFrame() # cluster data
    logger.ingo(f'Unique owners: {len(unique_owners)}')
    logger.info(f'Building super parcels for {fips} and dt {distance_threshold}...')
   
    all_superparcels = gpd.GeoDataFrame() # all super parcels
    logger.info(f'Starting Loop..')
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner].copy()

        for eps in distance_thresholds:
            clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=eps
            )

            if len(clusters) == 0:
                continue

            owner_parcels['cluster'] = clusters
            owner_parcels['cluster_area'] = owner_parcels['geometry'].area.astype(int)

            counts = owner_parcels['cluster'].value_counts()
            outlier_ids, clean_counts = segregate_outliers(counts, -1)

            cluster_filter = remove_from_df(
                df=owner_parcels,
                list_of_ids=outlier_ids,
                field='cluster'
            )

            if len(cluster_filter) == 0:
                continue

            total_area = cluster_filter.groupby('cluster')['cluster_area'].sum()

            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(clean_counts),
                p_area=cluster_filter['cluster'].map(total_area),
            )

            cluster_filter = cluster_filter[[key_field, 'puid', 'cluster', 'pcount', 'p_area', 'geometry']]
            cluster_filter['cluster_ID'] = cluster_filter[key_field] + '_' + cluster_filter['cluster'].astype(str)

            # CLUSTER IDS PUIDS (eg. cluster0: [p1, p2, p3], cluster1: [p4, p5])
            cluster_puid_gb = cluster_filter.groupby('cluster_ID')['puid'].apply(list).reset_index()

            sp = build_superparcels(
                df=cluster_filter,
                buffer=eps,
                dissolve_by='cluster_ID'
            )


            # CREATE HASED UNIQUE SP_ID
            sp['sp_id'] = cluster_puid_gb['puid'].apply(hash_puids)

            # ADD OTHER ATTRIBUTES
            sp = add_attributes(
                sp,
                fips=fips,
                sp_area=sp['geometry'].area,
                area_ratio=np.round(sp['p_area'] / sp['geometry'].area,1),
            )
            sp['sp_area'] = sp['sp_area'].astype(int)
            sp['p_area'] = sp['p_area'].astype(int)
            owner_area_ratio = sp['area_ratio'].values[0]

            if sp['area_ratio'].values[0] >= area_threshold:
                logger.info(f'Adding super parcel using dt {eps} with area ratio {owner_area_ratio}...')
                all_superparcels = pd.concat([all_superparcels, sp], ignore_index=True)
                break

    if len(all_superparcels) == 0:
        return None
    # REMOVE OVERLAPS
    logger.info('Removing overlaps...')
    all_superparcels = remove_overlap(all_superparcels)
    # REMOVE INVALID GEOMETRIESc
    logger.info(f'Shape before removing invalid geometries: {all_superparcels.shape}')
    logger.info('Removing invalid geometries...')
    all_superparcels, invalid_geoms = remove_invalid_geoms(all_superparcels)
    logger.info(f'Shape after removing invalid geometries: {all_superparcels.shape}')
    # FINAL TABLE
    all_superparcels = (
        all_superparcels[['fips', 'sp_id', 'cluster_ID', key_field, 'pcount', 'area_ratio', 'p_area', 'sp_area', 'cbi', 'geometry']]
        .to_crs(epsg=4326)
    )
    logger.info(f'Finished building super parcels for {fips} and dt {distance_thresholds}...')
    return all_superparcels
           

            
