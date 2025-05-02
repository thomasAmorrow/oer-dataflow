<img src="https://github.com/thomasAmorrow/oer-ega/blob/main/docs/logos/logo.png?raw=true" alt="vlogo" width="400"/>

# Exploration Gap Analysis

The NOAA Ocean Exploration Gap Analysis (EGA) is a tool to establish a spatial coverage baseline for ocean exploration data holdings, support the monitoring of exploration and characterization progress on previously unexplored ocean areas, and aid in the identification of priority areas for future expeditions and data collection efforts. At its core, the EGA is a PostGIS database synthesizing deep sea scientific observations from publicly available data archives.

# Methods and Tools

The EGA is built using several Docker containers. Data fetching, transformation, processing, and loading are handled in Python and SQL using Airflow as an orchestrator. Most tasks are lightweight and simple enough to be accomplished directly using an Airflow worker.

Each set of observations are spatially indexed to H3 hexagons at the level 5 resolution (~3.5 km wide hexagons). Each hexagon is assigned an Exploration Score based on the types of observations present in the hexagon at depths exceeding 200 m. Where an observation of the data type of interest exists, hexagons are flagged with a score of 1. Otherwise their score for that observation type is 0. A composite score for each hexagon is produced by averaging the scores across all observation types.

Using the nested heirarchy index of the H3 system, coarser hexagons (resolutions 4 and 3) are assigned scores based on the combination of child hexagon scores, yielding a heatmap of regions in the world oceans ranging from "well explored" to "unexplored".

# Inputs and Outputs

Inputs are defined for each ETL pipeline and currently include the following

|   Observation Type                                    |   Archive Source      |
| ------------------------------------------            | --------------------- |
|   Biological Occurrence Observations                  |   gbif.org            |
|   Geological Seafloor/Sub-seafloor Samples            |   ncei.noaa.gov/products/index-marine-lacustrine-samples  |
|   Environmental DNA (eDNA) Sequences                  |   obis.org            |
|   Water Biogeochemical Samles                         |   glodap.info         |
|   Seafloor Bathymetry Coverage (type identifier grid) |   gebco.net           |
|   Water Column Sonar Data                             |   ncei.noaa.gov/products/water-column-sonar-data  |

Additional data types and observations are being added.

Outputs currently comprise two GeoJSON file types

1. Hexagon files contain hexagon polygons at the defined resolution with exploration scores for each category as properties
2. Point files (smaller filesize) contain hexagon centroid points at the defined resolution with exploration scores for each category as properties

# How to Install and Run

Docker compose files for the custom containers and a Docker Compose YAML are provided. Current development and deployment occurs on an AWS EC2 Ubuntu instance, but it should be possible to deploy in almost any Docker environment. Some of the pathways (for example one of the directory references to an S3 bucket) are for this environment, but not dependent on AWS. Rename them for whatever makes sense in your organizational scheme.

Refer to Airflow documentation for specifics about setting up an Airflow environment. An .env sample document is included that can be modified for your own user/password/login purposes.

# Contributions

The Exploration Gap Analysis is developed and maintained by the NOAA Ocean Exploration Data Lab in the Science and Technology Division of NOAA Ocean Exploration. For questions, comments, or concerns, please reach out to NOAA Ocean Exploration.

**Interested in contributing? Feel like we're missing a critical set of publicly available observations that contribute to exploration of the unknown deep ocean? Reach out to us, or join us in developing this tool!**

# Future Work

The development branch (dev) is where the bulk of changes are happening. Check it out if you would like to keep up on the latest changes. Several other features are in various stages of development, but the intended immediate next steps are

1. Hosted output results for users and analysts (we run the code, you do amazing things next!)
2. Leaflet-based simple visualizations of EGA results
3. ArcGIS Experience Builder- and/or ArcGIS Online-ready exports
4. Developing more detailed scoring algorithms for the exploration scores, based on Subject Matter Expert input