Austin Breunig
January 16, 2025


# SuperParcels POC (Phase II.): Exploring Adaptive Epsilon in Urban Areas

## Introduction

In the context of parcel clustering, accurately determining the epsilon (ε) parameter for density-based clustering algorithms such as DBSCAN is critical. For rural parcels, a fixed epsilon of 200 meters is often sufficient, as parcel distances tend to be more uniform. However, this value is inappropriate for urban parcels, where distances between parcels are generally much smaller and more varied due to denser development and infrastructure.

To address this challenge, this report outlines a method for calculating an adaptive epsilon specific to parcels within a designated urban area, referred to here as a "place boundary." The approach is designed to improve the precision and relevance of clustering in urban environments, ensuring that the epsilon value better reflects the scale and spatial arrangement of urban parcels.

The following sections will compare the performance of this adaptive epsilon approach against the original fixed 200-meter epsilon on densely urban counties such as Los Angeles, Alameda, and San Francisco counties. We will examine the differences in clustering outcomes between the original and new methods, highlighting the improvements in precision and relevance for urban parcel clustering.

## Methodology

The proposed method involves a series of steps to compute and apply the adaptive epsilon. First, we apply k-means clustering to group parcels within the place boundary into regions of approximately 200 parcels per cluster. Next, a KD-Tree is built for each region to efficiently calculate the pairwise distances between parcels. These distances are then smoothed, and the "elbow" method is employed to identify a suitable epsilon value for each region. This epsilon is then used in the DBSCAN algorithm to identify same-owner parcels within each region.

Afterward, a merging process is used to expand the identification of same-owner parcels across adjacent regions, ensuring that all parcels with the same owner are correctly clustered. The final result is a set of same-owner clusters, each with a unique cluster ID, which are ultimately used to define the boundaries of the final superparcel.

This adaptive approach allows for more accurate and meaningful clustering in urban environments, accommodating the spatial complexities inherent in dense, urban parcel distributions. 

## Results
When comparing the original 200-meter epsilon to the adaptive epsilon approach, we see the adaptive method produces a significantly tighter and more localized boundary for each cluster.

#### Alameda
<img src="Images\phase2\Alameda2.png" width="1000" />

In contrast, the original 200-meter epsilon captures same-owner parcels that are far outside of a logical area, often including parcels that, while owned by the same entity, are separated by substantial distances. This can result in clusters that span across different neighborhoods or even parts of the county that have no direct proximity or connection, leading to over-generalization in the clustering process. The larger epsilon essentially groups parcels that are not realistically "close", distorting the final superparcel boundaries.

#### Los Angeles
<img src="Images\phase2\LA2_1.png" width="1000" />
<img src="Images\phase2\LA2_2.png" width="1000" />
<img src="Images\phase2\LA2_3.png" width="1000" />

On the other hand, the adaptive epsilon approach calculates a smaller, more appropriate epsilon value, reflecting the true spatial proximity of urban parcels. By utilizing k-means clustering to break the area into smaller, more manageable regions, and then refining the epsilon based on pairwise distances within these regions, the adaptive method ensures that only those same-owner parcels that are in close proximity are considered part of the same cluster. This leads to more accurate and relevant cluster boundaries, which are both smaller and more reflective of actual urban development patterns.

#### San Francisco
<img src="Images\phase2\SF2_1.png" width="1000" />
<img src="Images\phase2\SF2_2.png" width="1000" />

However, although the adaptive approach results in more accurately built superparcels, it also captures fewer superparcels overall. The original 200-meter epsilon, by contrast, produces many more superparcels, as its larger proximity range leads to the inclusion of a broader set of parcels in the same cluster. This brings up an important consideration: Could the adaptive approach be improved by refining the parameters further? Specifically, it may be possible to maintain the focus on local same-owner parcels while slightly expanding the proximity range to capture more same-owner parcels without compromising the overall accuracy and relevance of the clusters. Such a refinement could strike a balance between precision and coverage, allowing for a more comprehensive representation of same-owner parcel clusters in urban environments.

Ultimately, the adaptive approach demonstrates an ability to account for the denser, more complex parcel distributions typical of urban areas. It ensures that same-owner parcels are clustered in a way that is both geographically logical and consistent with urban realities. However, further refinement of the parameters could help capture more of the same-owner parcels without sacrificing the localized accuracy that the adaptive method provides.

### SuperParcel Comparison: Adaptive vs. Fixed
<img src="Images\phase2\area_comparison_adaptive_vs_original_Aquery.png" width="1000" />


### KNN Distance Comparison by Place Boundary
<img src="Images\phase2\knn_dist_comparison_adaptive_vs_original_Aquery.png" width="1000" />



## Insights and Recommendations
### Insights
#### Low vs. High KNN Distance Variability
When applying the adaptive epsilon approach to Los Angeles and San Francisco, we observe notable differences in the variability of the k-nearest neighbor (KNN) distances across the regions within each place boundary. These differences are indicative of the varying regional densities within each area, which directly impacts the clustering process.

In Los Angeles, there is a large degree of variability in KNN distances across different place boundaries and their respective regions. This variability reflects the significant differences in parcel density across the city. In more densely developed areas, the KNN distances are shorter, while in less developed areas, they tend to be larger. This variability is advantageous because it means that the adaptive epsilon approach can effectively capture the spatial distribution of parcels, using shorter distances in denser areas and larger distances in sparser ones. This localized approach ensures that the clustering algorithm more accurately represents the true spatial relationships between parcels, which is especially important in a sprawling, diverse city like Los Angeles.

However, the variability also presents some challenges. While the adaptive epsilon is tailored to regional densities, there is a risk that some distances may be either too large or too small, leading to over- or under-clustering of same-owner parcels. In certain cases, a distance that is too large might erroneously group parcels that are not sufficiently close, while a distance that is too small might fail to capture clusters that should logically be grouped together. This trade-off underscores the importance of carefully tuning the adaptive epsilon to balance accuracy and coverage, especially in areas with high regional variability.

In contrast, San Francisco, being a much smaller and more uniformly developed city, presents a different scenario. Since the entire city is treated as a single place boundary, the variability in KNN distances is minimal. This lack of variability suggests that San Francisco’s urban structure may be more consistent in terms of parcel density, with fewer fluctuations between densely and sparsely developed areas. Alternatively, it could be a result of the single large place boundary, which might have effectively smoothed out local density fluctuations, treating the entire city as one homogeneous area.

There are two possible explanations for this low variability. First, San Francisco could indeed exhibit a more uniform density across its territory, with few large differences in parcel distribution. Second, the single large place boundary could be suppressing the smaller-scale variations in density, effectively averaging out the local fluctuations. While this lack of variability simplifies the adaptive epsilon calculation, it also means that the clustering might not capture finer-grained patterns that could exist within smaller regions or neighborhoods.

In conclusion, while Los Angeles benefits from the ability to adapt to large regional variations in density, San Francisco's relatively uniform KNN distances reflect either a consistent density or the effect of a large, generalized place boundary. This highlights the different ways in which the adaptive epsilon approach can perform in cities with differing levels of spatial complexity. For cities with high variability, such as Los Angeles, the adaptive approach proves effective in reflecting true spatial relationships, though care must be taken to address the potential challenges of varying distances. In more uniform areas like San Francisco, the approach may smooth over these variations, leading to less variability but possibly losing some finer distinctions in the clustering.

### Recommendations
#### Refine Adaptive Epsilon Parameters
Given the trade-offs between accuracy and coverage observed in the adaptive epsilon approach, it is recommended to further refine the parameters to achieve a better balance between these two objectives. By adjusting the epsilon calculation method or incorporating additional factors, such as parcel size, it may be possible to capture more same-owner parcels while maintaining the localized precision of the clusters. This refinement could involve a more nuanced approach to distance calculation, potentially incorporating multiple epsilon values based on parcel characteristics or regional densities.

#### Implement Distance Thresholds Based on Regional Density
To address the challenges of varying KNN distances in Places with high density variability, it may be beneficial to implement distance thresholds based on regional density levels. By setting different epsilon values for areas with distinct parcel densities, we could build in gurad rails to prevent extreme over- or under-clustering. 
