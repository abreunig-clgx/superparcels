Austin Breunig  
Chris Tackett  
October 8, 2024

## SuperParcel POC: Exploring Spatial Clustering and Buffering for Owner Aggregation Boundaries

### Introduction

This project explored methodologies for clustering parcel data based on ownership and proximity, with the ultimate goal of creating superparcels—aggregations of parcels owned by the same individual within a specified distance. The Proof of Concept (PoC) focuses on using DBSCAN clustering and spatial analysis techniques to identify parcel groupings that reflect meaningful spatial associations between properties owned by the same entity.

A critical component of this project is the use of a precomputed distance matrix, which optimizes the clustering process by allowing for the efficient grouping of parcels based on proximity. Furthermore, this project refines the boundary creation around parcel clusters, moving from traditional methods like concave hulls to the more precise “reverse-buffer” technique, which produces tighter and more accurate superparcel boundaries.

Predefined guardrails were implemented to improve the quality of the resulting superparcels. One such filter was based on area, where only superparcels larger than 300,000 square meters were retained. This helped to emphasize more significant superparcels while discarding smaller ones that could introduce noise into the dataset.

Another guardrail involved setting the minimum sample size for DBSCAN to three parcels. This ensured that clusters contained at least three candidate parcels before boundary creation, preventing the formation of small, insignificant superparcel boundaries.

While the methodologies outlined in this PoC were successfully applied in more rural areas, where parcels are typically larger and more dispersed, challenges emerged in high-density urban environments, where the clustering parameters were less effective. Moreover, urban areas may not be the best geographical area for super parcels, as many of the urban AOIs resulted in very little superparcel creation. As such, the findings of this PoC offer valuable insights into the development of adaptive clustering techniques, and/or focusing on more rural areas.

The current recommended methodology relies on a subset of the parcel data, focusing on parcels with duplicate owners and unique geometries. Although this subset approach worked well for the initial PoC, further work is recommended to clean and adapt the data, potentially increasing the number of candidate parcels for more robust clustering outcomes.

### Preprocessing Step: Identifying Candidate Parcels

#### Overview

Before applying the clustering methodologies, it is essential to filter the dataset to identify candidate parcels for clustering. The focus is on parcels that have duplicate owners but unique geometries, as these represent parcels that will be most effective in understanding the clustering and boundary creation of the superparcels. 

#### Methodology

* Duplicate Owner, Unique Geometry Identification:  
  A process was conducted to classify parcels based on duplicates across multiple fields. The key was to identify parcels where the owner appears multiple times in the dataset, but the geometry of each parcel is unique.   
  Classifications  
  * Duplicate OWNER, Unique geometry         
  * Duplicate OWNER, Duplicate geometry  
  * Unique OWNER, Unique geometry            
  * Unique OWNER, Duplicate geometry       

#### Urban AOIs

San Francisco County, CA

![](Images\Urban\san_fran_parcel_candi.png)

Alameda County, CA

![](Images\Urban\alameda_parcel_candi.png)

Denver County, CO  
![](Images\Urban\denver_parcel_candi.png)

Dallas County, TX  
![](Images\Urban\dallas_parcel_candi.png)

#### Rural AOIs

Rusk County, WI

![](Images\Rural\rusk_wi_parcel_candi.png)

Kiowa County, KS  
![](Images\Rural\kiowa_ks_parcel_candi.png)

Crook County, OR  
![](Images\Rural\crook_or_parcel_candi.png)

Sierra County, NM  
![](Images\Rural\sierra_nm_parcel_candi.png)

### DBSCAN Clustering \+ Distance Matrix \+ Reverse Buffer

### Overview

The recommended methodology outlines a refined approach for clustering owner parcels based on proximity, utilizing DBSCAN with a precomputed distance matrix, and constructing superparcel boundaries using a reverse buffer technique. 

Recommended Methodology

* Precomputed Distance Matrix:  
  To enhance the performance and accuracy of the clustering process, a distance matrix was precomputed to capture the pairwise distances between parcels owned by the same individual. This matrix serves as input to DBSCAN, ensuring that the clustering is based on the actual spatial relationships between parcels, accounting for irregular parcel shapes and varying distances. Distances between parcels were calculated as “nearest-points” to each other.   
* DBSCAN Clustering:  
  DBSCAN, driven by the precomputed distance matrix, is used to cluster parcels within a specified proximity. Based on the previous findings, a 200-meter distance threshold is recommended for clustering, as this distance produced optimal results for grouping closely situated parcels without including those that are too far apart. The minimum sample size was set to three parcels meaning at least three parcels within the specified distance were required for a cluster to form.   
* Reverse Buffer for Boundary Construction:  
  To generate superparcel boundaries, the reverse buffer method is applied instead of the concave hull. This technique works by first creating an outward buffer around each parcel and then shrinking it inward, producing a boundary that tightly conforms to the cluster's outer edges. The reverse buffer yields a much more precise and compact boundary, providing an accurate representation of the grouped parcels within the specified distance threshold. This ensures that the resulting superparcels reflect the true spatial extent of the owner's properties, without excessive empty space or over-expansion.  
* Minimum Area: 										To generate significant superparcels, a minimum of 300,000 square meters was used to filter the final superparcel outputs. 

### Brief Overview of Other Explored Methodologies

1. DBSCAN Clustering with Concave Hull  
   One of the earlier methodologies involved using the concave hull to create boundaries around the clusters formed by DBSCAN. The concave hull method, while capable of forming boundaries that follow the general shape of the parcels, often produced loose boundaries that were not as tight or precise as needed for this PoC.  
   Additionally, centroids were initially used as inputs for clustering. This approach proved problematic for irregularly shaped parcels, as some centroids fell outside their respective parcels, negatively impacting the accuracy of the clustering process.  
2. KNN and Elbow Method for Distance Optimization  
   The K-Nearest Neighbors (KNN) algorithm was applied to determine the optimal distance for clustering by analyzing the elbow point in the plot of distances. However, the distances calculated were farther than intended for this PoC, which aimed to find similar parcels that were very close to each other. After testing with a 200-meter distance threshold, excellent results were achieved, making it the preferred method.  
3. Initial Clustering Without Preprocessing  
   An initial clustering attempt was made without the preprocessing step of filtering for duplicate owners with unique geometries. This resulted in noisy data and less relevant clusters, highlighting the importance of focusing only on parcels with distinct geometries under the same ownership for more meaningful clustering.

### 

### 

### Results and Considerations

Methodology Results:

* No SuperParcels for San Francisco County or Denver County:					Both counties resulted in zero superparcel creation, meaning, no one owner had more than 3 parcels within the distance threshold.   
* Efficient and Accurate Clustering:  
  The combination of the precomputed distance matrix and the DBSCAN algorithm effectively grouped nearby parcels, ensuring that only parcels within the 200-meter distance threshold were included in the clusters. This method avoided overestimating cluster distances, aligning the results with the intended goals of the PoC.  
* Tighter Boundaries with Reverse Buffer:  
  The reverse buffer method produced significantly tighter and more accurate boundaries around the clusters, compared to the concave hull. The boundaries closely followed the contours of the clustered parcels, resulting in superparcels that are a more faithful representation of the owner's parcels within the specified distance. This improvement enhances the precision of the parcel associations and the final superparcel output.  
* Performance:  
  * Alameda, CA: 38,644 parcels in approx. 15 minutes   
  * Dallas, TX: 44,050 parcels in approx. 27 minutes  
  * Crook, OR 1,226 Parcels in approx. 9 secs.  
  * Kiowa, KS 1,036 Parcels in approx. 7 secs.  
  * Rusk, Wi 4,028 Parcels in approx. 1 minute  
  * Sierra, NM 1,262 Parcels in approx. 12 secs.

Rural vs. Urban Areas:

* The 200-meter threshold worked well in rural areas, where superparcels were larger and more prevalent.  
* Rural areas had more "Duplicate Owner, Unique Geometry" candidate parcels, leading to a higher clustering success rate.  
* Urban areas had fewer candidate parcels after filtering, possibly due to:  
  * Higher instances of missing data.  
  * Less accurate owner information in dense urban environments.  
* The 200-meter distance threshold would not be appropriate for urban AOIs.

Preprocessing for Candidate Parcels:

* A key preprocessing step is identifying duplicate owners with unique geometries, which are used as candidate parcels for clustering. While effective, this limits the pool of parcels.

### Recommendations for Future Steps

* Adaptive Clustering for Diverse Regions:  
  Consider using adaptive distance thresholds in DBSCAN for regions with varying parcel densities. This could help maintain clustering accuracy across both dense urban areas and sparse rural regions. An example of this would be to run a KNN analysis on all parcel data prior to data cleaning, to understand local density. Once these “regions” were spatially identified, optimal distance could be applied to each region. Further research on this additional process would need to be considered. An obvious challenge to overcome would be if an owner existed in more than one region.   
* Robust Data-Cleaning Steps:  
  Develop additional data-cleaning steps to include more candidate parcels, improving clustering outcomes. These could help handle the incomplete, incorrect, or missing owner information. Additionally, handling the multiple Owner fields by standardizing a concatenation process could also help increase candidate parcels. 

### 

### Maps

![](Images\Urban\Alameda_map150.png)

![](Images\Urban\Dallas_map150.png)  

![](Images\Rural\Crook_OR_map150.png)  

![](Images\Rural\Kiowa_KS_map150.png) 

![](Images\Rural\Rusk_WI_map150.png)

![](Images\Rural\Sierra_NM_Map150.png)
