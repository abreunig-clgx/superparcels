import os
import glob
import geopandas as gpd
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Define the main processing function
def dt_owner_counts(data_dir, fips, dt_values, group_field, agg_field):
    
    
    all_gdfs = gpd.GeoDataFrame()
    for dt_value in dt_values:
        shp = find_shapefile(os.path.join(data_dir, fips), f'*{dt_value}*.shp')
        gdf = gpd.read_file(shp)
        gdf = add_field(gdf, group_field, dt_value)
        all_gdfs = pd.concat([all_gdfs, gdf], ignore_index=True)

    owner_counts = create_owner_counts(
        all_gdfs, 
        group_field=group_field, 
        agg_field=agg_field
    )
    owner_counts.index = [fips]

    return owner_counts
        

def find_shapefile(dir, pattern):
    try:
        shapefile = glob.glob(os.path.join(dir, pattern))[0]
    except:
        raise ValueError('No shapefiles found in {}'.format(os.path.join(dir, pattern)))
    return shapefile

def create_owner_counts(df, group_field, agg_field):
    owner_counts = df.groupby(group_field)[agg_field].nunique().reset_index().set_index(group_field).T
    owner_counts.columns.name = None
    return owner_counts

def add_field(df, field, value):
    df[field] = value
    return df