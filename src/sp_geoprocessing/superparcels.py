import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
import hashlib
import logging

logger = logging.getLogger(__name__)

def build_superparcels(df, buffer, dissolve_by='cluster_ID', area_threshold=None):
    """
    Dissolves clusters into super-parcels.
    Returns a GeoDataFrame with super-parcels.
    """
    #logger.info('Calculating mitre limit...')
    #df['mitre'] = df['geometry'].apply(compute_mitre_limit)

    #mitre_max = df.groupby(dissolve_by)['mitre'].max().reset_index()

    sp = df.dissolve(by=dissolve_by).reset_index()

    #sp['max_mitre'] = sp[dissolve_by].map(mitre_max.set_index(dissolve_by)['mitre'])
    
    #cross-boundary indicator
    logger.info('Calculating cross-boundary indicator...')
    sp['cbi'] = sp['geometry'].apply(lambda x: 1 if x.geom_type == 'MultiPolygon' else 0)

    

    logger.info('Applying buffer...')
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(buffer), axis=1)
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(-buffer), axis=1)
    
    if area_threshold:
        pass

    return sp


def compute_mitre_limit(polygon):
    """Compute the minimum mitre limit needed to avoid truncation."""
    if isinstance(polygon, Polygon):
        coords = np.array(polygon.exterior.coords)  # Get polygon coordinates
    else: # Handle MultiPolygon
        coords = [list(p.exterior.coords) for p in polygon.geoms]
        # Flatten list of coordinates
        coords = [item for sublist in coords for item in sublist]
        
    n = len(coords) - 1  # Ignore duplicate last point
    mitre_ratios = []

    for i in range(n):
        # Get three consecutive points (previous, current, next)
        p1, p2, p3 = coords[i - 1], coords[i], coords[(i + 1) % n]

        # Compute vectors
        v1, v2 = np.array(p1) - np.array(p2), np.array(p3) - np.array(p2)

        # Compute dot product and norms
        dot_product = np.dot(v1, v2)
        norm_v1, norm_v2 = np.linalg.norm(v1), np.linalg.norm(v2)
        norm_product = norm_v1 * norm_v2

        # Skip if vectors are degenerate (i.e., points are the same)
        if norm_product == 0:
            continue

        # Compute angle θ between vectors
        cos_theta = np.clip(dot_product / norm_product, -1, 1)  # Avoid precision errors
        theta = np.arccos(cos_theta)  # Angle in radians

        # Compute mitre ratio
        if theta > 0:  # Avoid division by zero
            mitre_ratio = 1 / np.sin(theta / 2)
            mitre_ratios.append(mitre_ratio)

    # Return max mitre ratio as the required mitre limit
    return max(mitre_ratios) if mitre_ratios else 2  # Default to 2


def hash_puids(puid_list):
    # puid integers are sorted integers
    puid_list = sorted(puid_list)
    joined = '-'.join([str(puid) for puid in puid_list])
    return hashlib.sha256(joined.encode()).hexdigest()[:10]


def remove_overlap(gdf):
    sindex = gdf.sindex
    result = []

    for idx, row in gdf.iterrows():
        geom = row.geometry
        possible_matches_index = list(sindex.intersection(geom.bounds)) # possible matches
        for other_idx in possible_matches_index:
            if other_idx != idx: # Avoid self-intersection
                other_geom = gdf.loc[other_idx].geometry # get the geometry of the other polygon
                if geom.intersects(other_geom) and geom.area >= other_geom.area:
                    geom = geom.difference(other_geom)
        if not geom.is_empty:
            new_row = row.copy()
            new_row.geometry = geom
            result.append(new_row)

    gdf_cleaned = gpd.GeoDataFrame(result, crs=gdf.crs)
    return gdf_cleaned

def remove_invalid_geoms(gdf, geom_type=['Polygon', 'MultiPolygon']):
    """
    Remove invalid geometries from a GeoDataFrame based on allowed geometry types.

    The function first explodes the GeoDataFrame to ensure all multi-part geometries are split into
    individual parts. It then identifies rows where the geometry type is not in the provided list
    (`geom_type`). It returns two GeoDataFrames: one with valid geometries and one with invalid geometries.

    Args:
        gdf (geopandas.GeoDataFrame): A GeoDataFrame containing geometries to be validated.
        geom_type (list, optional): List of allowed geometry types (default is ['Polygon', 'MultiPolygon']).

    Returns:
        tuple:
            - geopandas.GeoDataFrame: A GeoDataFrame with valid geometries.
            - geopandas.GeoDataFrame: A GeoDataFrame with invalid geometries, including selected columns.
    """

    invalid_geoms = gdf[gdf['geometry'].geom_type.isin(geom_type) == False]

    clean_gdf = gdf[~gdf.index.isin(invalid_geoms.index)]

    return clean_gdf, invalid_geoms

