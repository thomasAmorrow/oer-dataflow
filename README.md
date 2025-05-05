<p align="center">
  <img src="https://github.com/thomasAmorrow/oer-ega/blob/main/docs/logos/logo_banner.png?raw=true" alt="vlogo" width="800"/>
</p>


[![Docker](https://img.shields.io/badge/docker-ready-blue?logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/orchestrator-Airflow-017CEE?logo=apache-airflow)](https://airflow.apache.org/)
[![PostGIS](https://img.shields.io/badge/database-PostGIS-green?logo=postgresql)](https://postgis.net/)
[![License: GPL v3](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.en.html)
[![NOAA Ocean Exploration](https://img.shields.io/badge/NOAA-Ocean%20Exploration-005493)](https://oceanexplorer.noaa.gov)

##  Overview

 The **NOAA Ocean Exploration Gap Analysis (EGA)** is a tool to establish a spatial coverage baseline for ocean exploration data holdings, support the monitoring of exploration and characterization progress on previously unexplored ocean areas, and aid in the identification of priority areas for future expeditions and data collection efforts. At its core, the EGA is a PostGIS database synthesizing deep sea scientific observations from publicly available data archives.

## 📑 Table of Contents

- [Overview](#overview)
- [Methods and Tools](#-methods-and-tools)
- [Inputs and Outputs](#-inputs-and-outputs)
- [Installation and Usage](#-installation-and-usage)
- [Visuals](#-visuals)
- [Contributions](#-contributions)
- [Future Development](#-future-development)

## 🔧 Methods and Tools

The **EGA** leverages containerized workflows, orchestrated using **Apache Airflow**, with processing steps written in **Python** and **SQL**. All tasks are managed via lightweight Airflow workers.

Spatial data are indexed using the **H3 hexagonal grid system** at resolution 5. Each hexagon receives an **Exploration Score** based on the presence or absence of observation types deeper than 200 m:

- Score **1**: Observation type is present
- Score **0**: Observation type is absent

Averaging these per observation type yields a composite score. Scores at coarser H3 resolutions (4 and 3) are computed by aggregating child hex scores to generate a global "heatmap" of exploration status.

---

## 📥 Inputs and 📤 Outputs

### Inputs

| Observation Type                             | Data Source                                                             |
|---------------------------------------------|-------------------------------------------------------------------------|
| Biological Occurrence Observations           | [GBIF](https://www.gbif.org)                                            |
| Geological Seafloor/Sub-seafloor Samples     | [NCEI Marine & Lacustrine Samples](https://www.ncei.noaa.gov/products/index-marine-lacustrine-samples) |
| Environmental DNA (eDNA) Sequences           | [OBIS](https://obis.org)                                                |
| Water Biogeochemical Samples                 | [GLODAP](https://www.glodap.info)                                       |
| Seafloor Bathymetry Coverage (ID grid)       | [GEBCO](https://www.gebco.net)                                          |
| Water Column Sonar Data                      | [NCEI Water Column Sonar](https://www.ncei.noaa.gov/products/water-column-sonar-data) |

More types are in development and will be added in future releases.

### Outputs

- **Hexagon GeoJSON**: Full-resolution hex polygons with scores per observation type
- **Centroid GeoJSON**: Lighter-weight points for each hexagon with the same properties

---

## 🚀 Installation and Usage

EGA is deployed using **Docker Compose**, currently on **Ubuntu AWS EC2**, though it's compatible with any Docker-ready environment.

- All required Dockerfiles and a `docker-compose.yml` are included
- Some paths point to an S3 bucket but can be adapted to local filesystems
- Airflow requires manual setup—see [Airflow Docs](https://airflow.apache.org/docs/)
- A sample `.env` file is included for environment configuration

---

## 🖼️ Visuals

<p align="center">
  <img src="https://github.com/thomasAmorrow/oer-ega/blob/main/docs/maps/ResultsMap.png?raw=true" alt="vlogo" width="600"/>
</p>

GeoJSON results files can be visualized using a number of different tools. Here in ArcGIS Pro we show the nested heirarchy structure of level 5 resolution hexagons (upper) and level 4 resolution hexagons (lower). Composite scoring in the coarser hexagons depends on their contents and completely unexplored hexagons are highlighted as the highest priority for future exploration work.

> 📌 If you generate a compelling visualization or want to share use cases, submit a PR or email us to feature it here!

---

## 🤝 Contributions

This project is maintained by the **NOAA Ocean Exploration Data Lab** (Science & Technology Division) with help from the broader community.

**Interested in contributing?**  
Have data? Ideas? Feedback? Help us improve our understanding of the unknown deep ocean.

📬 Contact NOAA Ocean Exploration or open an issue/PR to get involved.

---

## 🔮 Future Development

The `dev` branch is the most active—follow for updates. Upcoming milestones include:

1. Publicly hosted EGA results for easy access
2. Leaflet-based web map viewer
3. ArcGIS-ready exports (Experience Builder, Online)
4. SME-driven enhancements to scoring methods

---
