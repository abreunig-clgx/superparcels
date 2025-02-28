
import os
import sys
import glob
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
import numpy as np
import cProfile
# ignore warnings
import warnings
warnings.filterwarnings('ignore')
import logging
from sp_geoprocessing.tools import *
from sp_geoprocessing.tools import setup_logger


# --- Exploratory Analysis imports ---
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import numpy as np

logger = setup_logger()
def build_sp_fixed(
    parcels, 
    fips,
    key_field='OWNER',
    distance_threshold=200, 
    sample_size=3,
    area_threshold=None,
    qa=False # if true, trigger cprofiler
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
    class TqdmToLogger:
        def write(self, message):
            # Avoid logging empty messages (e.g., newlines)
            message = message.strip()
            if message:
                logger.info(message)
        def flush(self):
            pass
    parcels = gpd.read_file(parcels)
    # setup cProfiler
    if qa:
        # enable cProfiler
        pass
    
    utm = parcels.estimate_utm_crs().to_epsg()
    parcels = parcels.to_crs(epsg=utm)  

    unique_owners = parcels[key_field].unique()

    clustered_parcel_data = gpd.GeoDataFrame() # cluster data
    #single_parcel_data = gpd.GeoDataFrame() # non-clustered data
    logger.info(f'Building super parcels for {fips} and dt {distance_threshold}...')
   
    for owner in unique_owners:
        owner_parcels = parcels[parcels[key_field] == owner] # ownder specific parcels
        
        # REFACTOR: CLUSTERING
        clusters = build_owner_clusters(
                owner_parcels,
                min_samples=sample_size,
                eps=distance_threshold
            )

        if len(clusters) == 0: # EMPTY: NO CLUSTERS
            #single_parcel_data = pd.concat([single_parcel_data, owner_parcels], ignore_index=True)  
            #single_parcel_data = add_attributes(
            #    single_parcel_data,
            #    #place_id=place_id,
            #    )
            continue

        owner_parcels['cluster'] = clusters # clustert ID
        owner_parcels['cluster_area'] = owner_parcels['geometry'].area
        counts = owner_parcels['cluster'].value_counts() # pd.series of cluster counts
        
        outlier_ids, clean_counts = segregate_outliers(counts, -1)

        #add_to_singles = locate_in_df(
        #    df=owner_parcels, 
        #    list_of_ids=outlier_ids, 
        #    field='cluster'
        #).drop(columns=['cluster', 'cluster_area'])

        #single_parcel_data = pd.concat([single_parcel_data, add_to_singles], ignore_index=True)

        #REFACTOR: WHAT FIELDS GO HERE??
        #if len(single_parcel_data) > 0:
        #    single_parcel_data = add_attributes(
        #        single_parcel_data,
        #        #place_id=place_id,
        #    )

        cluster_filter = remove_from_df(
            df=owner_parcels, 
            list_of_ids=outlier_ids, 
            field='cluster'
        )
        
        # REFACTOR: ADD ATTRIBUTES??
        if len(cluster_filter) > 0:
            cluster_filter = add_attributes(
                cluster_filter,
                pcount=cluster_filter['cluster'].map(clean_counts)
            )
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

    super_parcels = build_superparcels(
        df=clustered_parcel_data,
        buffer=distance_threshold,
        dissolve_by='cluster_ID',
    )

    # super parcel ID eg. owner + cluster ID + ID of each unique super parcel
    super_parcels['sp_id'] = super_parcels['cluster_ID'] + "_" + super_parcels.groupby('cluster_ID').cumcount().astype(str) 
    super_parcels = super_parcels[['sp_id', key_field, 'pcount', 'geometry']]

    logger.info(f'Finished building super parcels for {fips} and dt {distance_threshold}...')
    return super_parcels





# Define the compute_owner_stats function
def compute_owner_stats(gdf, model_name):
    owner_stats = gdf.groupby('owner').agg({
        'pcount': 'sum',
        'area': 'sum'
    }).reset_index()
    owner_stats['model'] = model_name
    return owner_stats

# Define the compare function to compute differences and percent changes
def compare(base, compare, on='owner', suffixes=('_base', '_compare')):
    comparison = pd.merge(base, compare, on=on, suffixes=(suffixes[0], suffixes[1]))
    comparison['pcount_diff'] = comparison[f'pcount{suffixes[0]}'] - comparison[f'pcount{suffixes[1]}']
    comparison['area_diff'] = comparison[f'area{suffixes[0]}'] - comparison[f'area{suffixes[1]}']
    comparison['area_pct_change'] = ((comparison[f'area{suffixes[1]}'] - comparison[f'area{suffixes[0]}']) /
                                      comparison[f'area{suffixes[0]}']) * 100
    return comparison

# Define the main processing function
def dt_exploratory_analysis(fips, data_dir):
    """
    Process shapefiles for a given FIPS code.
    
    It expects to find 6 shapefiles in a folder named after the FIPS code:
      * dt30, dt50, dt75, dt100, dt150, dt200
    and produces static plots (grid, histograms, boxplots) and an interactive map.
    
    Files are saved with filenames including the fips and dt information.
    """
    import os  # in-case not already imported
    # Ensure output directory exists
    # Build the folder path for the given FIPS code
    fips_folder = os.path.join(data_dir, fips)
    fips_output_dir = os.path.join(data_dir, fips, 'st_exploratory_analysis')
    os.makedirs(fips_output_dir, exist_ok=True)
    
    
    
    # Define the search patterns for each dt version
    patterns = {
        "dt30": "*dt30*.shp",
        "dt50": "*dt50*.shp",
        "dt75": "*dt75*.shp",
        "dt100": "*dt100*.shp",
        "dt150": "*dt150*.shp",
        "dt200": "*dt200*.shp"
    }
    
    # Load shapefiles into a dictionary
    gdfs = {}
    for dt, pattern in patterns.items():
        files = glob.glob(os.path.join(fips_folder, pattern))
        if files:
            gdfs[dt] = gpd.read_file(files[0])
        else:
            print(f"No shapefile found for {dt} in folder {fips_folder}.")
            return
    
    # --- Plot 1: Grid layout of the 6 shapefiles ---
    fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 10))
    colors = {
        "dt30": 'lightblue',
        "dt50": 'lightgreen',
        "dt75": 'lightcoral',
        "dt100": 'plum',
        "dt150": 'lightyellow',
        "dt200": 'lightgrey'
    }
    dt_order = ["dt30", "dt50", "dt75", "dt100", "dt150", "dt200"]
    for ax, dt in zip(axes.flatten(), dt_order):
        gdfs[dt].plot(ax=ax, color=colors[dt], edgecolor='black')
        ax.set_title(f'{fips}-{dt}: {len(gdfs[dt])} superparcels')
        ax.set_axis_off()
    plt.tight_layout()
    grid_fn = os.path.join(fips_output_dir, f"{fips}_grid.png")
    plt.savefig(grid_fn)
    plt.close()
    
    # --- Project all GeoDataFrames to a common UTM CRS ---
    utm_crs = gdfs["dt30"].estimate_utm_crs().to_epsg()
    for dt in gdfs:
        gdfs[dt] = gdfs[dt].to_crs(epsg=utm_crs)
    
    # --- Calculate area for each GeoDataFrame ---
    for dt in gdfs:
        gdfs[dt]['area'] = gdfs[dt].geometry.area
    
    # --- Compute owner-level statistics ---
    stats = {}
    for dt in gdfs:
        stats[dt] = compute_owner_stats(gdfs[dt], dt)
    
    # Combine stats into a single DataFrame for pivoting
    combined_stats = pd.concat(stats.values())
    
    # Pivot for parcel counts and areas
    pivot_pcount = combined_stats.pivot(index='owner', columns='model', values='pcount')
    pivot_area = combined_stats.pivot(index='owner', columns='model', values='area')
    
    print(pivot_pcount.columns)
    print(pivot_area.columns)



    # Calculate percentage change relative to dt 200 (baseline)
    pct_change_pcount = ((pivot_pcount.subtract(pivot_pcount['dt200'], axis=0)
                          .divide(pivot_pcount['dt200'], axis=0)) * 100)
    pct_change_area = ((pivot_area.subtract(pivot_area['dt200'], axis=0)
                        .divide(pivot_area['dt200'], axis=0)) * 100)
    
    # --- Plot 2: Histograms for pct change (parcel count and area) ---
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    for model in pct_change_pcount.columns:
        if model != 'dt200':
            sns.histplot(pct_change_pcount[model].dropna(), bins=50, ax=axes[0],
                         label=model, alpha=0.6)
    axes[0].axvline(0, color='black', linestyle='--', label='dt 200 Baseline')
    axes[0].set_title('Percent Change in Parcel Count Relative to dt 200')
    axes[0].set_xlabel('% Change in pcount')
    axes[0].set_ylabel('Frequency')
    axes[0].set_yscale('log')
    axes[0].legend()
    
    for model in pct_change_area.columns:
        if model != 'dt200':
            sns.histplot(pct_change_area[model].dropna(), bins=50, ax=axes[1],
                         label=model, alpha=0.6)
    axes[1].axvline(0, color='black', linestyle='--', label='dt 200 Baseline')
    axes[1].set_title('Percent Change in Area Relative to dt 200')
    axes[1].set_xlabel('% Change in Area')
    axes[1].set_ylabel('Frequency')
    axes[1].set_yscale('log')
    axes[1].legend()
    
    plt.tight_layout()
    hist_fn = os.path.join(fips_output_dir, f"{fips}_histograms.png")
    plt.savefig(hist_fn)
    plt.close()
  
    # --- Compute comparisons using the compare function ---
    compare_results = {}
    for dt in ["dt30", "dt50", "dt75", "dt100", "dt150"]:
        compare_results[dt] = compare(stats["dt200"], stats[dt], suffixes=('_dt200', f'_{dt}'))
        compare_results[dt]['model'] = dt
    combined_compare = pd.concat(compare_results.values())
    
    
    # --- Plot 3: Boxplots for area percent change and parcel count difference ---
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=combined_compare, x='model', y='area_pct_change')
    plt.title('Area Percent Change Relative to dt 200')
    plt.ylabel('Area Percent Change')
    plt.xlabel('Model')
    box_area_fn = os.path.join(fips_output_dir, f"{fips}_boxplot_area.png")
    plt.savefig(box_area_fn)
    plt.close()
    
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=combined_compare, x='model', y='pcount_diff')
    plt.title('Parcel Count Difference Relative to dt 200')
    plt.ylabel('Parcel Count Difference')
    plt.xlabel('Model')
    box_pcount_fn = os.path.join(fips_output_dir, f"{fips}_boxplot_pcount.png")
    plt.savefig(box_pcount_fn)
    plt.close()
    
    # --- Create an interactive map for dt30 differences ---
    # Merge dt30 stats with its geometry so we can display differences
    gdf_dt30_merged = gdfs["dt30"].merge(compare_results["dt30"], on='owner')
    gdf_dt50_merged = gdfs["dt50"].merge(compare_results["dt50"], on='owner')
    gdf_dt75_merged = gdfs["dt75"].merge(compare_results["dt75"], on='owner')
    gdf_dt100_merged = gdfs["dt100"].merge(compare_results["dt100"], on='owner')
    gdf_dt150_merged = gdfs["dt150"].merge(compare_results["dt150"], on='owner')



    pcount_interactives = [gdf_dt30_merged, gdf_dt50_merged, gdf_dt75_merged, gdf_dt100_merged, gdf_dt150_merged]

    dt200_interactive = gdfs["dt200"].to_crs(epsg=4326)

    for id, gdf in enumerate(pcount_interactives):
        gdf_filtered = gdf[gdf['pcount_diff'] != 0].to_crs(epsg=4326)

        m_pcount = gdf_filtered.explore(
            column='pcount_diff',
            cmap='coolwarm',
            tooltip=['owner', 'pcount_diff'],
            legend=True,
            name='Parcel Count Diff'
        )
        
        # Add an HTML title to the interactive map
        title_html = f'<h3 align="center" style="font-size:20px"><b>Parcel Count Difference ({dt_order[id]} vs dt200) for {fips}</b></h3>'
        m_pcount.get_root().html.add_child(folium.Element(title_html))
        
        # Define a tooltip for the baseline dt200 layer
        tooltip_baseline = folium.GeoJsonTooltip(
            fields=['owner', 'pcount'],
            aliases=['Owner:', 'Parcel Count (dt200):'],
            localize=True,
        )
        
        # Add the dt200 baseline layer interactively (so tooltips work)
        folium.GeoJson(
            dt200_interactive,
            name='Baseline dt200',
            tooltip=tooltip_baseline,
            style_function=lambda feature: {
                'fillOpacity': 0,
                'color': '#555555',
                'weight': 2,
            },
            interactive=True
        ).add_to(m_pcount)
        
        # Also add the dt30 merged layer (non-interactive)
        folium.GeoJson(
            gdf.to_crs(epsg=4326),
            name=dt_order[id],
            style_function=lambda feature: {
                'fillOpacity': 0.2,
                'color': '#6699CC',
                'weight': 2
            },
            interactive=False
        ).add_to(m_pcount)
        
        folium.LayerControl().add_to(m_pcount)
        
        # Save the interactive map as HTML
        interactive_fn = os.path.join(fips_output_dir, f"{fips}_interactive_dt30.html")
        m_pcount.save(interactive_fn)
        
        print(f"Processing for FIPS {fips} complete. Files saved in {fips_output_dir}.")

    




