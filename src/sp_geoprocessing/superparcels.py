import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
import hashlib
import logging

logger = logging.getLogger(__name__)


def build_superparcels(df, buffer, dissolve_by='cluster_ID', area_threshold=None):
    """
    Dissolve parcel clusters into super-parcels and apply buffering operations.

    This function dissolves input parcel clusters based on a specified grouping field (default is 'cluster_ID') 
    to create super-parcels. It then applies a positive buffer followed by a negative buffer to smooth the 
    boundaries of the dissolved geometries. Additionally, a cross-boundary indicator (CBI) is calculated 
    by checking if a resulting geometry is a MultiPolygon (indicator of a cross-boundary parcel).

    Parameters
    ----------
    df : geopandas.GeoDataFrame
        Input GeoDataFrame containing parcel clusters. Must include a 'geometry' column and a column matching 
        the dissolve_by parameter.
    buffer : float
        The buffer distance to apply. The geometry is first expanded using this value and then contracted 
        by the same value.
    dissolve_by : str, optional
        The column name to group parcels for dissolving. Default is 'cluster_ID'.
    area_threshold : float, optional
        An optional area threshold that can be used to filter the resulting super-parcels. This parameter is 
        reserved for future logic if needed.

    Returns
    -------
    geopandas.GeoDataFrame
        A GeoDataFrame of super-parcels containing the dissolved geometries, an added 'cbi' column indicating 
        cross-boundary parcels, and updated geometries after buffering.
    """
    # Dissolve clusters based on the specified field.
    sp = df.dissolve(by=dissolve_by).reset_index()

    # Compute a cross-boundary indicator: 1 if MultiPolygon, 0 otherwise.
    sp['cbi'] = sp['geometry'].apply(lambda x: 1 if x.geom_type == 'MultiPolygon' else 0)

    # Apply buffer to smooth geometry boundaries: first positive then negative.
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(buffer), axis=1)
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(-buffer), axis=1)

    # Optionally, area filtering can be implemented here if area_threshold is provided.
    if area_threshold:
        pass

    return sp


def compute_mitre_limit(polygon):
    """
    Compute the minimum mitre limit required to avoid truncation when buffering geometries.

    The mitre limit is based on the angles at the vertices of a polygon. For each vertex,
    the function calculates a mitre ratio defined as 1/sin(theta/2), where theta is the 
    angle between two consecutive edges at that vertex. The highest mitre ratio among all vertices 
    is returned as the required mitre limit. For MultiPolygons, the function processes each component,
    then flattens and evaluates the coordinates.

    Parameters
    ----------
    polygon : shapely.geometry.Polygon or shapely.geometry.MultiPolygon
        The input polygon or multi-polygon for which the mitre limit is to be computed.

    Returns
    -------
    float
        The computed mitre limit value. Returns a default value of 2 if no valid mitre ratio can be computed.
    """
    if isinstance(polygon, Polygon):
        coords = np.array(polygon.exterior.coords)  # Get polygon coordinates
    else:  # Handle MultiPolygon
        coords = [list(p.exterior.coords) for p in polygon.geoms]
        # Flatten list of coordinates.
        coords = [item for sublist in coords for item in sublist]

    n = len(coords) - 1  # Ignore duplicate last point.
    mitre_ratios = []

    for i in range(n):
        # Get three consecutive points (previous, current, next).
        p1, p2, p3 = coords[i - 1], coords[i], coords[(i + 1) % n]

        # Compute vectors from the current point to previous and next points.
        v1, v2 = np.array(p1) - np.array(p2), np.array(p3) - np.array(p2)

        # Compute dot product and norms.
        dot_product = np.dot(v1, v2)
        norm_v1, norm_v2 = np.linalg.norm(v1), np.linalg.norm(v2)
        norm_product = norm_v1 * norm_v2

        # Skip degenerate cases (e.g., duplicate points).
        if norm_product == 0:
            continue

        # Compute the angle between vectors.
        cos_theta = np.clip(dot_product / norm_product, -1, 1)  # Avoid precision errors.
        theta = np.arccos(cos_theta)  # Angle in radians.

        # Compute mitre ratio; avoid division by zero.
        if theta > 0:
            mitre_ratio = 1 / np.sin(theta / 2)
            mitre_ratios.append(mitre_ratio)

    # Return the maximum mitre ratio as the limit, or default to 2 if no values were computed.
    return max(mitre_ratios) if mitre_ratios else 2


def hash_puids(puid_list):
    """
    Generate a unique hash for a sorted list of parcel unique identifiers (puids).

    The function sorts the list, concatenates the puids into a single string separated by hyphens,
    and computes a SHA-256 hash of the resulting string. The resulting hexadecimal hash is truncated
    to the first 10 characters.

    Parameters
    ----------
    puid_list : list of int or list of str
        A list of parcel unique identifiers.

    Returns
    -------
    str
        A hexadecimal string (of length 10) representing the hashed puid list.
    """
    puid_list = sorted(puid_list)
    joined = '-'.join([str(puid) for puid in puid_list])
    return hashlib.sha256(joined.encode()).hexdigest()[:10]


def remove_overlap(gdf):
    """
    Remove overlapping areas from geometries in a GeoDataFrame.

    For each geometry in the input GeoDataFrame, this function identifies potentially overlapping geometries 
    using a spatial index. If an overlap is detected and the current geometry is larger than the overlapping 
    geometry, the overlapping area is subtracted (using the difference operation) from the current geometry.
    The cleaned geometries are then reassembled into a new GeoDataFrame.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        Input GeoDataFrame containing potentially overlapping geometries.

    Returns
    -------
    geopandas.GeoDataFrame
        A new GeoDataFrame with overlapping areas removed from the geometries.
    """
    sindex = gdf.sindex
    result = []

    for idx, row in gdf.iterrows():
        geom = row.geometry
        possible_matches_index = list(sindex.intersection(geom.bounds))
        # Process potential overlaps from each candidate geometry.
        for other_idx in possible_matches_index:
            if other_idx != idx:  # Skip self-comparison.
                other_geom = gdf.loc[other_idx].geometry
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
    Remove geometries from a GeoDataFrame that are not of the allowed types.

    The function checks each geometry in the input GeoDataFrame against a list of allowed geometry types.
    Geometries that do not match one of the allowed types are considered invalid and separated into an invalid 
    subset. The function logs the number of invalid and valid geometries and returns a tuple containing two 
    GeoDataFrames: one with valid geometries and one with invalid geometries.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        A GeoDataFrame containing geometries to be validated.
    geom_type : list of str, optional
        List of allowed geometry types. Default is ['Polygon', 'MultiPolygon'].

    Returns
    -------
    tuple
        A tuple containing:
          - geopandas.GeoDataFrame: GeoDataFrame with valid geometries.
          - geopandas.GeoDataFrame: GeoDataFrame with invalid geometries.
    """
    invalid_geoms = gdf[gdf['geometry'].geom_type.isin(geom_type) == False]
    clean_gdf = gdf[~gdf.index.isin(invalid_geoms.index)]
    
    logger.info(f'Invalid geometries removed: {len(invalid_geoms)}')
    logger.info(f'Valid geometries remaining: {len(clean_gdf)}')

    return clean_gdf, invalid_geoms
