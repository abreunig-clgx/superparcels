import os
import glob
import geopandas as gpd
import pandas as pd
import numpy as np
import logging
from sp_geoprocessing.superparcels import remove_invalid_geoms

logger = logging.getLogger(__name__)


def dt_overlap(gdf, sp_id_field, owner_field):
    """
    Calculate spatial overlap metrics for each unique distance threshold (dt) in the GeoDataFrame.

    This function processes a GeoDataFrame containing parcel data that includes a 'dt' column. For each unique 
    dt value, it:
      - Converts the data to a projected coordinate system (using the estimated UTM CRS).
      - Removes invalid geometries and cleans the data.
      - Performs a spatial join to identify overlapping geometries.
      - Filters out overlapping records where the owner fields do not match.
      - Computes the intersection area between overlapping geometries.
      - Aggregates overlap metrics including total overlaps, count of super parcels, percentage of overlaps, 
        and average overlaps per record.
      - Compiles these metrics into a summary DataFrame (with the FIPS code as the index).

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame that must include the columns 'fips', 'dt', and a valid 'geometry' column.
    sp_id_field : str
        The name of the field that contains the unique super parcel identifier. This is expected to be used 
        with a suffix (e.g., sp_id_field+'_left') in the spatial join result.
    owner_field : str
        The name of the field used to denote the owner. This field is used to filter overlapping pairs 
        based on owner mismatch.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the FIPS code as index and columns for each dt value containing:
          - {dt_value}_overlaps: Total number of overlapping pairs (sum of unique overlapping indices).
          - {dt_value}_sp_count: Number of cleaned super parcels for that dt.
          - {dt_value}_pct_overlap: Percentage of super parcels that have overlaps.
          - {dt_value}_avg_overlap: Average number of overlaps per super parcel.
    """
    utm_crs = gdf.estimate_utm_crs().to_epsg()
    all_dt_dfs = pd.DataFrame()
    fips = gdf['fips'].unique()[0]
    for dt_value in gdf['dt'].unique():
        gdf_dt = gdf[gdf['dt'] == dt_value].reset_index()
        gdf_dt = gdf_dt.to_crs(utm_crs)
        
        logger.info(f'Overlaps from dt-{dt_value} for {fips}')
        print(gdf_dt.columns)
        print(gdf_dt.shape)
        
        # Remove invalid geometries and filter non-null geometries
        gdf_dt, _ = remove_invalid_geoms(gdf_dt)
        gdf_dt = gdf_dt[gdf_dt['geometry'].notnull()]
        gdf_dt_cleaned = gdf_dt[gdf_dt['geometry'].is_valid].copy()
        gdf_dt_cleaned['geometry'] = gdf_dt_cleaned['geometry'].buffer(0)
        
        try:
            # Perform spatial join to find overlapping geometries
            sjoin = gpd.sjoin(gdf_dt_cleaned, gdf_dt_cleaned, how='left', predicate='overlaps')
            print(sjoin.shape)
        except Exception as e:
            logger.error(f"Spatial join failed: {e}")
            continue
        
        # Filter overlaps where owner information is mismatched
        mismatch = sjoin[sjoin[owner_field + '_left'] != sjoin[owner_field + '_right']]
        mismatch = mismatch[mismatch['owner_right'].notnull()]
        mismatch = mismatch[['index_left', sp_id_field + '_left', owner_field + '_left',
                             'index_right', owner_field + '_right', 'geometry']]
        
        # Merge to obtain geometries for the right side of the join
        sjoin_right = pd.merge(
            mismatch,
            gdf_dt_cleaned[['index', owner_field, 'geometry']],
            left_on='index_right', right_on='index', how='inner'
        )
       
        sjoin_right['diff_area'] = sjoin_right.apply(
            lambda x: x['geometry_x'].intersection(x['geometry_y']).area
                      if x['geometry_x'].is_valid and x['geometry_y'].is_valid else np.nan,
            axis=1
        )
     
        overlaps = sjoin_right[sjoin_right['diff_area'].notnull()]
        overlaps = sjoin_right[sjoin_right['diff_area'] > 1]
        print(overlaps.shape)
        
        # Group and compute metrics
        groupby_counts = overlaps.groupby('index_left')['index_right'].nunique()
        data_dict = {
            f'{dt_value}_overlaps': groupby_counts.sum(),
            f'{dt_value}_sp_count': len(gdf_dt_cleaned),
            f'{dt_value}_pct_overlap': groupby_counts.sum() / len(gdf_dt_cleaned) * 100,
            f'{dt_value}_avg_overlap': groupby_counts.mean()
        }
        df = pd.DataFrame(data_dict, index=[0])
        df.index = [fips]
        all_dt_dfs = pd.concat([all_dt_dfs, df], axis=1)
        
    return all_dt_dfs


def dt_owner_counts(gdf, group_field):
    """
    Calculate the count of unique owners for each unique distance threshold (dt) in the GeoDataFrame.

    For each dt value present in the input GeoDataFrame, this function computes the number of unique owner groups 
    based on the provided grouping field. It aggregates the results in a DataFrame with the FIPS code as the index.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame that must include a 'dt' column and an owner/group field.
    group_field : str
        The field used to group the data, typically representing owner information.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with the FIPS code as the index and one column per dt value containing the number 
        of unique owner groups.
    """
    all_owner_counts = pd.DataFrame()
    fips = gdf['fips'].unique()[0]
    for dt_value in gdf['dt'].unique():
        logger.info(f'Owner Counts from dt-{dt_value} for {fips}')
        gdf_dt = gdf[gdf['dt'] == dt_value]
        owner_counts = get_owner_counts(gdf_dt, group_field=group_field)
        data = {f'{dt_value}': owner_counts}
        df = pd.DataFrame(data, index=[0])
        df.index = [fips]
        all_owner_counts = pd.concat([all_owner_counts, df], axis=1)
    return all_owner_counts


def find_shapefile(dir, pattern):
    """
    Find a shapefile in the given directory matching the specified pattern.

    This function uses the glob module to search for files that match the given filename pattern 
    within the specified directory. It returns the first match found.

    Parameters
    ----------
    dir : str
        The directory path in which to search for shapefiles.
    pattern : str
        The filename pattern to search for (e.g., '*.shp').

    Returns
    -------
    str
        The file path to the first matching shapefile.

    Raises
    ------
    ValueError
        If no shapefile matching the pattern is found in the specified directory.
    """
    try:
        shapefile = glob.glob(os.path.join(dir, pattern))[0]
    except IndexError:
        raise ValueError('No shapefiles found in {}'.format(os.path.join(dir, pattern)))
    return shapefile


def get_owner_counts(df, group_field):
    """
    Compute the number of unique owner groups in the DataFrame.

    This function groups the DataFrame by the specified group field and returns the count of unique groups.
    The count is determined from the number of unique rows after applying the groupby operation.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing the owner or group information.
    group_field : str
        The field used to group the DataFrame.

    Returns
    -------
    int
        The number of unique owner groups found.
    """
    owner_counts = df.groupby(group_field).nunique().shape[0]
    return owner_counts


def add_field(df, field, value):
    """
    Add a new field to the DataFrame and assign it a constant value.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to which the new field will be added.
    field : str
        The name of the new field (column) to add.
    value : any
        The constant value to assign to the entire column.

    Returns
    -------
    pandas.DataFrame
        The modified DataFrame with the added field.
    """
    df[field] = value
    return df


def dt_area_ratio(gdf, area_field):
    """
    Filter the GeoDataFrame by an area ratio threshold.

    This function returns a subset of the input GeoDataFrame where the values in the specified area field
    are less than or equal to 1. This is often used to filter out records where the area ratio exceeds a 
    predetermined threshold.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame containing the area field.
    area_field : str
        The name of the field representing the area ratio.

    Returns
    -------
    geopandas.GeoDataFrame
        A filtered GeoDataFrame containing only records with an area ratio less than or equal to 1.
    """
    gdf = gdf[gdf[area_field] <= 1]
    return gdf
