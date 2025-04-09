import os
import glob
import geopandas as gpd
import pandas as pd
import numpy as np
import logging
from sp_geoprocessing.superparcels import remove_invalid_geoms

logger = logging.getLogger(__name__)

def dt_overlap(gdf, sp_id_field, owner_field):
    all_dt_dfs = pd.DataFrame()
    fips = gdf['fips'].unique()[0]
    for dt_value in gdf['dt'].unique():
        gdf_dt = gdf[gdf['dt'] == dt_value].reset_index()
        logger.info(f'Overlaps from dt-{dt_value} for {fips}')
        
        gdf_dt, _ = remove_invalid_geoms(gdf_dt)
        # remove invlaid geometries
        gdf_dt = gdf_dt[gdf_dt['geometry'].notnull()]
        gdf_dt_cleaned = gdf_dt[gdf_dt['geometry'].is_valid].copy()
        gdf_dt_cleaned['geometry'] = gdf_dt_cleaned['geometry'].buffer(0)
        try:
            sjoin = gpd.sjoin(gdf_dt_cleaned, gdf_dt_cleaned, how='left', predicate='overlaps')
        except Exception as e:
            logger.error(f"Spatial join failed: {e}")
            continue
        mismatch = sjoin[sjoin[owner_field+'_left'] != sjoin[owner_field+'_right']]
        mismatch = mismatch[mismatch['owner_right'].notnull()]
        mismatch = mismatch[['index_left', sp_id_field+'_left', owner_field+'_left', 'index_right', owner_field+'_right', 'geometry']]

        sjoin_right = pd.merge(mismatch, gdf_dt_cleaned[['index', owner_field, 'geometry']], left_on='index_right', right_on='index', how='inner')
        
        sjoin_right['diff_area'] = sjoin_right.apply(
            lambda x: x['geometry_x'].intersection(x['geometry_y']).area if x['geometry_x'].is_valid and x['geometry_y'].is_valid else np.nan, axis=1)

        overlaps = sjoin_right[sjoin_right['diff_area'].notnull()]
        overlaps = sjoin_right[sjoin_right['diff_area'] > 1]
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

# Define the main processing function
def dt_owner_counts(gdf, group_field):
    all_owner_counts = pd.DataFrame()
    fips = gdf['fips'].unique()[0]
    for dt_value in gdf['dt'].unique():
        logger.info(f'Owner Counts from dt-{dt_value} for {fips}')
        gdf_dt = gdf[gdf['dt'] == dt_value]

        owner_counts = get_owner_counts(
            gdf_dt, 
            group_field=group_field
        )
        data = {
            f'{dt_value}': owner_counts
        }
        df = pd.DataFrame(data, index=[0])
        df.index = [fips]
     
        all_owner_counts = pd.concat([all_owner_counts, df], axis=1)
    return all_owner_counts
        
        
def find_shapefile(dir, pattern):
    try:
        shapefile = glob.glob(os.path.join(dir, pattern))[0]
    except:
        raise ValueError('No shapefiles found in {}'.format(os.path.join(dir, pattern)))
    return shapefile

def get_owner_counts(df, group_field):
    owner_counts = df.groupby(group_field).nunique().shape[0]
    return owner_counts

def add_field(df, field, value):
    df[field] = value
    return df

def dt_area_ratio(gdf, area_field):

    gdf = gdf[gdf[area_field] <= 1]
    return gdf
        
        
