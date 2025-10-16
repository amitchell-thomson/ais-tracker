# **AIS Analytics for Commodity Spread Prediction**
Geospatial data pipeline and ML framework for transforming global vessel traffic into commodity market signals

## Table of Contents
- [About](#about)
- [Features](#features)
- [Examples/Notebooks](#examplesnotebooks)
- [TO-DO](#to-do)


## About
This project is an end-to-end geospatial data engineering and machine learning pipeline designed to transform **Automatic Identification System (AIS)** vessel traffic data into meaningful commodity market signals.

The system **ingests raw AIS feeds** (terrestrial only for reliability) into a Postgres/TimescaleDB + PostGIS database, **eventises vessel movements** (eg. port/shipping-lane exits/entries) and **applies feature engineering techniques** to construct simple supply/demand proxies such as port liftings, approach congestion and shipping lane flows

These engineered datasets form the basis for **predictive modelling** - for example, estimating crude oil trade flows and testing signals for Brent-WTI spread movemens.

The project combines:
- Data Engineering -> ingestion pipelines, schema design, retention/compression policies
- Geospacial analytics -> polygon-based area detection, directional gates
- Feature engineering -> event inference, continuous aggregates, exponential weighted moving averages, congestion metrics

Still under development, next steps are finishing feature engineering to finish with ML ready features

## Features
- ### *Ingestion pipeline (terrestrial AIS)*
    - Raw data ingested into **puclic.ais_fix** 
    - Stores **time series data per vessel**
    - Cycle through tiles, restricted to **only** tiles that surround areas of interest
    - Deduplication/filter logic:
        1. *(collector)* - only keep if **lat/lon** present
        2. *(collector)* - remove if ships are far from **bounding box** of areas
        3. *(database)* - skip insert if no meaningful data about ship has **changed**
        4. *(database)* - skip if **unrealistic** movement (derived speed to high)
    #### Database Table: **public.ais_fix**
    
    

    | Column | Description |
    | ------------------ | ------------------------------------------------------------------------------------------- |
    | `ts`               | Observation timestamp (hypertable time), `timestamptz NOT NULL`.                            |
    | `src`              | Data source, one of `'terrestrial'` or `'sat'`.                                             |
    | `vessel_uid`       | Stable vessel ID (IMO/MMSI or hashed surrogate).                                            |
    | `lat`              | Latitude in degrees, must be between −90 and 90.                                            |
    | `lon`              | Longitude in degrees, must be between −180 and 180.                                         |
    | `sog`              | Speed over ground (knots).                                                                  |
    | `cog`              | Course over ground (degrees 0–360), may be `NULL`.                                          |
    | `heading`          | True heading (degrees 0–360), may be `NULL`.                                                |
    | `elapsed`          | Minutes since last message per feed.                                                        |
    | `destination`      | Free-form destination text.                                                                 |
    | `flag`             | Vessel flag.                                                                                |
    | `length_m`         | Hull length in meters.                                                                      |
    | `width_m`          | Beam in meters.                                                                             |
    | `dwt`              | Deadweight in tonnes.                                                                       |
    | `shipname`         | Vessel name (may be `"[SAT-AIS]"` for SAT tracks).                                          |
    | `shiptype`         | AIS ship type code.                                                                         |
    | `ship_id`          | Provider ship ID when available.                                                            |
    | `rot`              | Rate of turn (degrees per minute), optional.                                                |
    | `geom`             | Point geometry from `lat/lon`, `geometry(Point, 4326)`; can be derived in a BEFORE trigger. |
    | `area_id_core`     | `area.area_id` for core port polygon if inside.                                             |
    | `in_core`          | Boolean: inside a core port polygon.                                                        |
    | `area_id_approach` | `area.area_id` for approach polygon if inside.                                              |
    | `in_approach`      | Boolean: inside an approach polygon.                                                        |
    | `lane_id`          | `area.area_id` for corridor/chokepoint if inside.                                           |
    | `in_lane`          | Boolean: inside a corridor/chokepoint polygon.                                              |
    | `gate_id`          | `area_gate.gate_id` if intersecting a gate.                                                 |
    | `gate_end`         | Side label for the gate, one of `'west'`, `'east'`, `'north'`, `'south'`.                   |

- ### *Geospatial eventisation*
- ### *TimescaleDB continuous aggregates for flows and congestion*


## Examples/Notebooks

## TO-DO