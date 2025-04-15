"""
Module: sp_build

This module contains functions to build super parcels from candidate parcel data
using clustering techniques. Two main methods are provided:
  - build_sp_fixed: Clusters parcels using a fixed distance threshold (epsilon)
    for DBSCAN clustering, processes and aggregates clusters, and then builds
    super parcels.
  - build_sp_multi: Iterates over a list of distance thresholds (epsilons) to attempt
    clustering for each owner, selecting the first clustering result that meets a given
    area ratio condition. The function then builds super parcels using the accepted result.

The module relies on several functions imported from other parts of the project,
such as build_owner_clusters, build_superparcels, hash_puids, remove_overlap,
remove_invalid_geoms, add_attributes, remove_from_df, and segregate_outliers.
"""

import pandas as pd
import numpy as np
import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')
import logging

from sp_geoprocessing.cluster import build_owner_clusters, build_sindex_owner_clusters
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
    add_attributes  # note: duplicate import to be consolidated if needed
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
    Build super parcels using fixed-distance clustering.

    This function implements the process of clustering candidate parcels belonging to the same owner 
    using DBSCAN with a fixed epsilon (distance_threshold). For each owner, the function:
      1. Resets the parcel index and adds a unique parcel identifier ('puid').
      2. Transforms the coordinate reference system (CRS) of the parcels to the estimated UTM.
      3. Iterates over all unique owner values (from key_field) to perform clustering via DBSCAN.
      4. Removes outlier clusters (if any) and calculates cluster-level attributes such as count and 
         total area.
      5. Concatenates the clustered data from each owner.
      6. Uses the aggregated cluster data to build super parcels using a geometric buffering and dissolve 
         operation.
      7. Generates a unique super parcel identifier (sp_id) by hashing the list of parcel IDs in each cluster.
      8. Adds additional attributes including fips code, area measurements, and area ratio.
      9. Removes any overlapping or invalid geometries and converts the final GeoDataFrame to EPSG:4326.

    Parameters
    ----------
    parcels : geopandas.GeoDataFrame
        The input GeoDataFrame containing candidate parcels. It is expected to have a valid 'geometry' column.
    fips : str
        FIPS code identifier for the county or region under consideration.
    key_field : str, optional
        The column name used to identify owner information for clustering. Default is 'OWNER'.
    distance_threshold : int, optional
        The DBSCAN epsilon value (in the same unit as the parcel CRS) used as the distance threshold 
        for clustering. Default is 200.
    sample_size : int, optional
        Minimum number of samples required for DBSCAN clustering. Default is 3.
    area_threshold : int or None, optional
        Minimum area threshold for accepting or further processing clusters. Currently, if provided, it is
        reserved for future filtering logic (not fully implemented). Default is None.

    Returns
    -------
    geopandas.GeoDataFrame or None
        A GeoDataFrame of super parcels with columns such as fips, sp_id, cluster_ID, the owner field,
        parcel count (pcount), area ratio, parcel area (p_area), super parcel area (sp_area), cbi, and geometry.
        The GeoDataFrame is projected to EPSG:4326. If no clusters are generated, the function returns None.
    """
    # Reset index and assign a unique parcel identifier
    parcels = parcels.reset_index(drop=True)
    parcels['puid'] = parcels.index

    # Estimate UTM and convert CRS accordingly
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    # Identify unique owners for clustering
    unique_owners = parcels[key_field].unique()
    clustered_parcel_data = gpd.GeoDataFrame()  # Container for clustered data

    logger.info(f'Building super parcels for {fips} with distance threshold {distance_threshold}...')
   
    # Process parcels for each owner
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner]
        
        # Build clusters using DBSCAN with fixed epsilon.
        clusters = build_owner_clusters(
            owner_parcels,
            min_samples=sample_size,
            eps=distance_threshold
        )

        # Skip if no clusters were found
        if len(clusters) == 0:
            continue

        owner_parcels['cluster'] = clusters
        owner_parcels['cluster_area'] = owner_parcels['geometry'].area.astype(int)
        counts = owner_parcels['cluster'].value_counts()  # Count of parcels per cluster
        
        # Identify outlier clusters and retain only valid clusters
        outlier_ids, clean_counts = segregate_outliers(counts, -1)
        cluster_filter = remove_from_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        )
        
        if len(cluster_filter) > 0:
            total_area = cluster_filter.groupby('cluster')['cluster_area'].sum()
            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(clean_counts),
                p_area=cluster_filter['cluster'].map(total_area),
            )
            cluster_filter = cluster_filter[[key_field, 'puid', 'cluster', 'pcount', 'p_area', 'geometry']]
            clustered_parcel_data = pd.concat([clustered_parcel_data, cluster_filter], ignore_index=True)

    # Return None if no clusters have been generated
    if len(clustered_parcel_data) == 0:
        return None

    # Create a composite cluster identifier
    clustered_parcel_data['cluster_ID'] = (
        clustered_parcel_data[key_field] + '_' +
        clustered_parcel_data['cluster'].astype(str)
    )

    # Area threshold filtering reserved for future implementation
    if area_threshold:
        # Future area threshold filtering logic can be added here
        pass
    
    # Group parcel IDs for each cluster to later hash them into a unique super parcel id
    cluster_puid_gb = clustered_parcel_data.groupby('cluster_ID')['puid'].apply(list).reset_index()
    
    # Generate super parcels using dissolve and buffering methods
    super_parcels = build_superparcels(
        df=clustered_parcel_data,
        buffer=distance_threshold,
        dissolve_by='cluster_ID',
    )

    # Create hashed unique super parcel identifier
    super_parcels['sp_id'] = cluster_puid_gb['puid'].apply(hash_puids)

    # Add additional attributes such as fips, super parcel area, and area ratio
    super_parcels = add_attributes(
        super_parcels,
        fips=fips,
        sp_area=super_parcels['geometry'].area,
        area_ratio=super_parcels['p_area'] / super_parcels['geometry'].area,
    )
    super_parcels['sp_area'] = super_parcels['sp_area'].astype(int)
    super_parcels['p_area'] = super_parcels['p_area'].astype(int)

    # Remove overlapping geometries
    logger.info('Removing overlaps...')
    super_parcels = remove_overlap(super_parcels)

    # Remove invalid geometries and log shape changes
    logger.info(f'Shape before removing invalid geometries: {super_parcels.shape}')
    logger.info('Removing invalid geometries...')
    super_parcels, invalid_geoms = remove_invalid_geoms(super_parcels)
    logger.info(f'Shape after removing invalid geometries: {super_parcels.shape}')

    # Select final columns and convert to EPSG:4326
    super_parcels = (
        super_parcels[['fips', 'sp_id', 'cluster_ID', key_field, 'pcount', 'area_ratio', 'p_area', 'sp_area', 'cbi', 'geometry']]
        .to_crs(epsg=4326)
    )

    logger.info(f'Finished building super parcels for {fips} with distance threshold {distance_threshold}.')
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
    Build super parcels by iterating over multiple distance thresholds.

    This function attempts to cluster candidate parcels for each owner using a list
    of distance thresholds (epsilons). For each owner, it iterates over the provided thresholds
    until a clustering result meets a specified area ratio condition (if an area_threshold is set).
    The steps include:
      1. Resetting the parcel index and assigning a unique parcel identifier ('puid').
      2. Estimating and converting the CRS to the appropriate UTM projection.
      3. For each unique owner, attempting clustering with each distance threshold:
         - Clustering is done using DBSCAN (via build_owner_clusters).
         - Outlier clusters are identified and removed.
         - Cluster-level attributes (parcel count and total area) are calculated.
         - A composite cluster identifier is formed.
         - Super parcels are built using a dissolve (buffer) method.
         - A unique super parcel id (sp_id) is generated by hashing the aggregated parcel identifiers.
         - Additional attributes including the fips code, super parcel area, and area ratio are added.
         - If the area ratio from the first attempt meets or exceeds the area_threshold,
           the clustering result is accepted. Otherwise, a second attempt is permitted.
      4. Finally, overlapping and invalid geometries are removed, and the resulting GeoDataFrame 
         is transformed to EPSG:4326.

    Parameters
    ----------
    parcels : geopandas.GeoDataFrame
        The input GeoDataFrame containing candidate parcels. Must include a valid 'geometry' column.
    fips : str
        FIPS code identifier for the region or county.
    key_field : str, optional
        The column name representing the owner field used for clustering. Default is 'OWNER'.
    distance_thresholds : list of int, optional
        A list of DBSCAN epsilon values to attempt for clustering. Default is [200].
    sample_size : int, optional
        Minimum number of samples required for DBSCAN clustering. Default is 3.
    area_threshold : int or None, optional
        Minimum area ratio threshold used to determine the acceptance of the first clustering attempt.
        If not met, a second pass using a different threshold is allowed. Default is None.

    Returns
    -------
    geopandas.GeoDataFrame or None
        A GeoDataFrame of super parcels with columns including fips, sp_id, cluster_ID, the owner field,
        pcount, area_ratio, p_area, sp_area, cbi, and geometry. The data is reprojected to EPSG:4326.
        If no valid clusters are produced, the function returns None.
    """
    logger.info('Building super parcels with multiple distance thresholds...')
    parcels = parcels.reset_index(drop=True)
    parcels['puid'] = parcels.index

    # Estimate UTM and transform CRS
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  
    logger.info(f'Using UTM CRS {utm}...')
    unique_owners = parcels[key_field].unique()
    logger.info(f'Found {len(unique_owners)} unique owners.')
    logger.info(f'Building super parcels for {fips} with thresholds: {distance_thresholds}...')
   
    all_superparcels = gpd.GeoDataFrame()  # Container for all resulting super parcels

    # Process each owner individually
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner].copy()
        first_pass_attempted = False

        # Iterate over each provided distance threshold (epsilon)
        for eps in distance_thresholds:
            clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=eps
            )

            # Skip if no clusters are formed
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

            # If no valid clusters remain, try the next threshold
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

            # Group parcel IDs by cluster for hashing later
            cluster_puid_gb = cluster_filter.groupby('cluster_ID')['puid'].apply(list).reset_index()

            # Build super parcels for the current threshold
            sp = build_superparcels(
                df=cluster_filter,
                buffer=eps,
                dissolve_by='cluster_ID'
            )

            # Generate a unique super parcel identifier
            sp['sp_id'] = cluster_puid_gb['puid'].apply(hash_puids)

            # Add additional attributes: fips, super parcel area and area ratio
            sp = add_attributes(
                sp,
                fips=fips,
                sp_area=sp['geometry'].area,
                area_ratio=np.round(sp['p_area'] / sp['geometry'].area, 1),
            )
            sp['sp_area'] = sp['sp_area'].astype(int)
            sp['p_area'] = sp['p_area'].astype(int)
            owner_area_ratio = sp['area_ratio'].values[0]

            # Determine if the first attempt's result is acceptable based on area_threshold
            if not first_pass_attempted:
                if owner_area_ratio >= area_threshold:
                    all_superparcels = pd.concat([all_superparcels, sp], ignore_index=True)
                    break
                else:
                    first_pass_attempted = True
            else:
                # Collect result from a second pass if necessary
                all_superparcels = pd.concat([all_superparcels, sp], ignore_index=True)
                break

    if len(all_superparcels) == 0:
        return None

    # Remove overlapping parcels
    logger.info('Removing overlaps...')
    all_superparcels = remove_overlap(all_superparcels)
    # Remove invalid geometries and log changes
    logger.info(f'Shape before removing invalid geometries: {all_superparcels.shape}')
    logger.info('Removing invalid geometries...')
    all_superparcels, _ = remove_invalid_geoms(all_superparcels)
    logger.info(f'Shape after removing invalid geometries: {all_superparcels.shape}')

    # Select final desired columns and convert CRS to EPSG:4326
    all_superparcels = (
        all_superparcels[['fips', 'sp_id', 'cluster_ID', key_field, 'pcount', 'area_ratio', 'p_area', 'sp_area', 'cbi', 'geometry']]
        .to_crs(epsg=4326)
    )
    logger.info(f'Finished building super parcels for {fips} with thresholds: {distance_thresholds}.')
    return all_superparcels


def build_sp_multi_optimized(
    parcels, 
    fips,
    key_field='OWNER',
    distance_thresholds=[200], 
    sample_size=3,
    area_threshold=None,
):
    """
    Build super parcels by iterating over multiple distance thresholds.

    This function attempts to cluster candidate parcels for each owner using a list
    of distance thresholds (epsilons). For each owner, it iterates over the provided thresholds
    until a clustering result meets a specified area ratio condition (if an area_threshold is set).
    The steps include:
      1. Resetting the parcel index and assigning a unique parcel identifier ('puid').
      2. Estimating and converting the CRS to the appropriate UTM projection.
      3. For each unique owner, attempting clustering with each distance threshold:
         - Clustering is done using DBSCAN (via build_owner_clusters).
         - Outlier clusters are identified and removed.
         - Cluster-level attributes (parcel count and total area) are calculated.
         - A composite cluster identifier is formed.
         - Super parcels are built using a dissolve (buffer) method.
         - A unique super parcel id (sp_id) is generated by hashing the aggregated parcel identifiers.
         - Additional attributes including the fips code, super parcel area, and area ratio are added.
         - If the area ratio from the first attempt meets or exceeds the area_threshold,
           the clustering result is accepted. Otherwise, a second attempt is permitted.
      4. Finally, overlapping and invalid geometries are removed, and the resulting GeoDataFrame 
         is transformed to EPSG:4326.

    Parameters
    ----------
    parcels : geopandas.GeoDataFrame
        The input GeoDataFrame containing candidate parcels. Must include a valid 'geometry' column.
    fips : str
        FIPS code identifier for the region or county.
    key_field : str, optional
        The column name representing the owner field used for clustering. Default is 'OWNER'.
    distance_thresholds : list of int, optional
        A list of DBSCAN epsilon values to attempt for clustering. Default is [200].
    sample_size : int, optional
        Minimum number of samples required for DBSCAN clustering. Default is 3.
    area_threshold : int or None, optional
        Minimum area ratio threshold used to determine the acceptance of the first clustering attempt.
        If not met, a second pass using a different threshold is allowed. Default is None.

    Returns
    -------
    geopandas.GeoDataFrame or None
        A GeoDataFrame of super parcels with columns including fips, sp_id, cluster_ID, the owner field,
        pcount, area_ratio, p_area, sp_area, cbi, and geometry. The data is reprojected to EPSG:4326.
        If no valid clusters are produced, the function returns None.
    """
    try:
        logger.info('Building super parcels with multiple distance thresholds...')
        parcels = parcels.reset_index(drop=True)
        parcels['puid'] = parcels.index

        # Estimate UTM and transform CRS
        utm = parcels.estimate_utm_crs().to_epsg()
        parcels = parcels.to_crs(epsg=utm)  
        logger.info(f'Using UTM CRS {utm}...')
    
        all_superparcels = gpd.GeoDataFrame()  # Container for all resulting super parcels

        logger.info('Using area threshold: {}'.format(area_threshold))
        
        first_pass_attempted = False
        sp_second_pass = gpd.GeoDataFrame()  # Container for second pass super parcels
        # Iterate over each provided distance threshold (epsilon)
        for eps in distance_thresholds:
            if not sp_second_pass.empty:
                parcels = sp_second_pass.copy()
            logger.info(f'Clustering parcels for {fips} with threshold: {eps}...')
            logger.info(parcels.shape)
            candidate_clusters = (
                parcels.groupby(key_field)
                .apply(build_sindex_owner_clusters, sample_size=sample_size, threshold=eps)
                .reset_index(drop=True)
            )

            # Skip if no clusters were formed
            if len(candidate_clusters) == 0:
                logger.info(f'No clusters found for {fips} with threshold: {eps}.')
                continue
            
            candidate_clusters['cluster_ID'] = candidate_clusters[key_field] + '_' + candidate_clusters['cluster'].astype(str)
            candidate_clusters['cluster_area'] = candidate_clusters['geometry'].area.astype(int)

            total_area = candidate_clusters.groupby('cluster_ID')['cluster_area'].sum()
            cluster_pcount = candidate_clusters['cluster_ID'].value_counts()

            candidate_clusters = add_attributes(
                candidate_clusters,
                pcount=candidate_clusters['cluster_ID'].map(cluster_pcount),
                p_area=candidate_clusters['cluster_ID'].map(total_area),
            )
            candidate_clusters = candidate_clusters[[key_field, 'puid', 'cluster_ID', 'pcount', 'p_area', 'geometry']]
            

            # Group parcel IDs by cluster for hashing later
            cluster_puid_gb = candidate_clusters.groupby('cluster_ID')['puid'].apply(list).reset_index()

            # Build super parcels for the current threshold
            logger.info(f'Building super parcels for {fips} with threshold: {eps}...')
            logger.info(candidate_clusters.shape)
            
            sp = build_superparcels(
                df=candidate_clusters,
                buffer=eps,
                dissolve_by='cluster_ID'
            )
            logger.info(sp.shape)
            # Generate a unique super parcel identifier
            sp['sp_id'] = cluster_puid_gb['puid'].apply(hash_puids)

            logger.info(f'Generated {sp.shape[0]} super parcels for {fips} with threshold: {eps}.')
            # Add additional attributes: fips, super parcel area and area ratio
            sp = add_attributes(
                sp,
                fips=fips,
                sp_area=sp['geometry'].area,
                area_ratio=np.round(sp['p_area'] / sp['geometry'].area, 1),
            )
            logger.info(sp['area_ratio'].describe())
            sp['sp_area'] = sp['sp_area'].astype(int)
            sp['p_area'] = sp['p_area'].astype(int)
            

            # Determine if the first attempt's result is acceptable based on area_threshold
            if not first_pass_attempted:
                logger.info('First pass attempt...')
                sp_first_pass = sp.loc[sp['area_ratio'] >= area_threshold]
                logger.info(f'First pass super parcels: {sp_first_pass.shape}')
                sp_second_pass = sp.loc[sp['area_ratio'] < area_threshold]
                logger.info(f'Second pass super parcels: {sp_second_pass.shape}')
                logger.info(f'First pass super parcels: {sp_first_pass.shape}')
                all_superparcels = pd.concat([all_superparcels, sp_first_pass], ignore_index=True)
                first_pass_attempted = True 

                if sp_second_pass.empty: # No second pass needed, everything in first pass
                    logger.info('No second pass needed, all parcels accepted.')
                    break
            else:
                logger.info('Second pass attempt...')
                # Collect result from a second pass if necessary
                all_superparcels = pd.concat([all_superparcels, sp_second_pass], ignore_index=True)
                break

        if len(all_superparcels) == 0:
            logger.info(f'No valid super parcels generated for {fips}.')
            return None

        # Remove overlapping parcels
        logger.info('Removing overlaps...')
        all_superparcels = remove_overlap(all_superparcels)
        # Remove invalid geometries and log changes
        logger.info(f'Shape before removing invalid geometries: {all_superparcels.shape}')
        logger.info('Removing invalid geometries...')
        all_superparcels, _ = remove_invalid_geoms(all_superparcels)
        logger.info(f'Shape after removing invalid geometries: {all_superparcels.shape}')

        # Select final desired columns and convert CRS to EPSG:4326
        all_superparcels = (
            all_superparcels[['fips', 'sp_id', 'cluster_ID', key_field, 'pcount', 'area_ratio', 'p_area', 'sp_area', 'cbi', 'geometry']]
            .to_crs(epsg=4326)
        )
        logger.info(f'Finished building super parcels for {fips} with thresholds: {distance_thresholds}.')
        return all_superparcels
    except Exception as e:
        logger.error(f'Error building super parcels for {fips}: {e}')
        return None
