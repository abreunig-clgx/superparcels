import numpy as np
from shapely.geometry import Polygon
import hashlib
import logging

logger = logging.getLogger(__name__)

def build_superparcels(df, buffer, dissolve_by='cluster_ID', area_threshold=None):
    """
    Dissolves clusters into super-parcels.
    Returns a GeoDataFrame with super-parcels.
    """
    sp = df.dissolve(by=dissolve_by).reset_index()
    #cross-boundary indicator
    logger.info('Calculating cross-boundary indicator...')
    sp['cbi'] = sp['geometry'].apply(lambda x: 1 if x.geom_type == 'MultiPolygon' else 0)

    logger.info('Calculating mitre limit...')
    sp['mitre'] = sp['geometry'].apply(compute_mitre_limit)

    logger.info('Applying buffer...')
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(buffer, join_style=2, mitre_limit=x['mitre']), axis=1)
    sp['geometry'] = sp.apply(lambda x: x.geometry.buffer(-buffer, join_style=2, mitre_limit=x['mitre']), axis=1)
    
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


