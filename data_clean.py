
import os
import geopandas as gpd
import pandas as pd
from shapely import wkt

data_path = r'D:\Projects\superparcels\data\abreunig_temp_sp_sample_08013.csv'

data_dir = r'D:\Projects\superparcels\data'

df = pd.read_csv(data_path)

gdf = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=4326)
gdf.to_file(os.path.join(data_dir, 'sp_sample_08013.shp'))


# Identify duplicate owners, addresses, and geometries
df['duplicate_owner'] = df.duplicated(subset=['OWNER'], keep=False)
df['duplicate_address'] = df.duplicated(subset=['std_addr'], keep=False)
df['duplicate_geometry'] = df.duplicated(subset=['geometry'], keep=False)

# Create a classification column based on duplication status (with geometry)
df['classification'] = df.apply(
    lambda row: (
        'Class1: Duplicate Owner, Address & Geometry' if row['duplicate_owner'] and row['duplicate_address'] and row['duplicate_geometry'] else
        'Class2: Duplicate Owner & Address, Unique Geometry' if row['duplicate_owner'] and row['duplicate_address'] and not row['duplicate_geometry'] else
        'Class3: Duplicate Owner & Unique Address, Geometry' if row['duplicate_owner'] and not row['duplicate_address'] and not row['duplicate_geometry'] else
        'Class4: Unique Owner & Duplicate Address & Geometry' if not row['duplicate_owner'] and row['duplicate_address'] and row['duplicate_geometry'] else
        'Class5: Unique Owner & Address, Duplicate Geometry' if not row['duplicate_owner'] and not row['duplicate_address'] and row['duplicate_geometry'] else
        'Class6: Unique Owner & Address & Geometry'
    ), axis=1
)

# Count the occurrences of each classification
matrix = df['classification'].value_counts()

print(matrix)


# create clasification codes
df['classification_code'] = df['classification'].apply(lambda x: x.split(':')[0][5])
df['classification_code'] = df['classification_code'].astype(int)
df['classification_code'].value_counts()



class_codes = [1,2,3,4,5,6]
for class_code in class_codes:
    code = df[df['classification_code'] == class_code]
    code.to_file(os.path.join(input_dir, f'sp_sample_fips_code{class_code}.shp'))


cluster_canidate_codes = [2,3] # classes after dissolving
cluster = df[df['classification_code'].isin(cluster_canidate_codes)]
cluster.to_file(os.path.join(input_dir, f'sp_sample_{fips}_cluster_canidates.shp'))




