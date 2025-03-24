import os
import glob
import geopandas as gpd
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def dt_overlap(data_dir, fips, dt_values, sp_id_field, owner_field):

    all_dt_dfs = pd.DataFrame()
    for dt_value in dt_values:
        logger.info(f'Processing {dt_value} for {fips}')
        shp = find_shapefile(os.path.join(data_dir, fips), f'*dt{dt_value}*.shp')
        gdf = gpd.read_file(shp).reset_index()
        
        sjoin = gpd.sjoin(gdf, gdf, how='left', predicate='overlaps')
        mismatch = sjoin[sjoin[owner_field+'_left'] != sjoin[owner_field+'_right']]
        mismatch = mismatch[mismatch['owner_right'].notnull()]
        mismatch = mismatch[['index_left', sp_id_field+'_left', owner_field+'_left', 'index_right', owner_field+'_right', 'geometry']]

        sjoin_right = pd.merge(mismatch, gdf[['index', owner_field, 'geometry']], left_on='index_right', right_on='index', how='inner')
        
        sjoin_right['diff_area'] = sjoin_right.apply(
            lambda x: x['geometry_x'].intersection(x['geometry_y']).area if x['geometry_x'].is_valid and x['geometry_y'].is_valid else np.nan, axis=1)

        overlaps = sjoin_right[sjoin_right['diff_area'].notnull()]
        overlaps = sjoin_right[sjoin_right['diff_area'] > 1]
        groupby_counts = overlaps.groupby('index_left')['index_right'].nunique()

        data_dict = {
            f'{dt_value}_overlaps': groupby_counts.sum(),
            f'{dt_value}_sp_count': len(gdf),
            f'{dt_value}_pct_overlap': groupby_counts.sum() / len(gdf) * 100,
            f'{dt_value}_avg_overlap': groupby_counts.mean()
        }

        df = pd.DataFrame(data_dict, index=[0])
        df.index = [fips]

        all_dt_dfs = pd.concat([all_dt_dfs, df], axis=1)
        
    return all_dt_dfs

# Define the main processing function
def dt_owner_counts(data_dir, fips, dt_values, group_field):
    all_owner_counts = pd.DataFrame()

    for dt_value in dt_values:
        shp = find_shapefile(os.path.join(data_dir, fips), f'*dt{dt_value}*.shp')
       
        gdf = gpd.read_file(shp)

        owner_counts = get_owner_counts(
            gdf, 
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