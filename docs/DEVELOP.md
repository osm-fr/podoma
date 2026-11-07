# Running of Podoma

## Dependencies

- NodeJS >= 24
- Bash tools : curl, mawk, grep, sed, bc
- PostgreSQL >= 15
- Python 3 (and `requests` module)
- [Osmium](https://osmcode.org/osmium-tool/) > 1.18
- [osmctools](https://wiki.openstreetmap.org/wiki/Osmupdate)
- [Imposm](https://imposm.org/) >= 3
- [pg_tileserv](https://github.com/CrunchyData/pg_tileserv)
- Dependencies of [sendfile_osm_oauth_protector](https://github.com/geofabrik/sendfile_osm_oauth_protector#requirements)

### Osmium building

Podoma requires a recent version of osmium as it takes advantage to newest tags-filter abilities.
Not many Linux distros got the appropriate package available in their repositories and you may need to build your own binary of osmium.

See guidelines on the [official README](https://github.com/osmcode/osmium-tool/blob/master/README.md#building).

Following packets on debian can be useful

- build-essential
- cmake
- zlib1g-dev
- libbz2-dev
- liblz4-dev
- libboost-dev
- libboost-program-options-dev

## Getting started

```bash
git clone https://github.com/osm-fr/podoma.git
cd Podoma
git submodule update --init
```

## General configuration

The general configuration of the tool is to be filled in `config.json`. There is a suggested model in the `config.example.json` file. The parameters are as follows:

- `WEBSITE_NAME`: website displayable name
- `WEBSITE_MOTTO`: website displayable short description
- `WEBSITE_URL`: front-end URL (for SEO)
- `WEBSITE_DESCRIPTION`: website description (for SEO)
- `OSM_USER`: OpenStreetMap username for retrieving the modification history with metadata
- `OSM_PASS`: password associated with the OSM user account
- `OSM_CLIENT_ID` : client ID associated with the OSM account
- `OSH_PBF_AUTHORIZED`: Enable OAuth2 authentication to download OSH files when necessary. To be disabled if the repository selected in `OSH_PBF_URL` doesn't support it.
- `OSH_PBF_URL`: URL of the OSH.PBF file (history and metadata, example `https://osm-internal.download.geofabrik.de/europe/france/reunion-internal.osh.pbf`)
- `OSM_PBF_URL`: URL of the OSM.PBF file (current state, example `https://download.geofabrik.de/europe/france-latest.osm.pbf`). This file isn't covered by authorization process.
- `POLY_URL`: URL of a polygon file holding the perimeter in which projects are considered (example `https://download.geofabrik.de/europe/france.poly`). This file isn't covered by authorization process.
- `DB_USE_IMPOSM_UPDATE` : enable or disabled Imposm3 integration (to use an existing database which would be maintained by other means, by default `true`)
- `WORK_DIR`: download and temporary storage folder (must have capacity to store the OSH PBF file, example `/tmp/pdm`)
- `OSM_URL`: OpenStreetMap instance to use (example `https://www.openstreetmap.org`)
- `OSM_API_URL` : API OpenStreetMap instance to use (example `https://www.api.openstreetmap.org`)
- `JOSM_REMOTE_URL`: address of the JOSM server to reach (example `http://localhost:8111`)
- `OVERPASS_URL`: URL towards the interpreter of an Overpass-API instance to be used for missing members retrieval
- `OVERPASS_FATAL`: whether a failed Overpass request should abort `update_changes`. If false the script will continue processing the remaining projects with the missing members omitted and only a warning will be logged; THIS WILL INTRODUCE ERRORS in the statistics, prefer changing `OVERPASS_URL` to a more stable instance if possible.
- `OSMOSE_URL`: Osmose instance to use (example `https://osmose.openstreetmap.fr`)
- `NOMINATIM_URL`: instance of Nominatim to use (example `https://nominatim.openstreetmap.org`)
- `MAPILLARY_URL`: Mapillary instance to use (example `https://www.mapillary.com`)
- `MAPILLARY_API_KEY`: Mapillary API key ([could be created here](https://www.mapillary.com/dashboard/developers))
- `PANORAMAX_URL` : Panoramax instance to use (example `https://api.panoramax.xyz`)
- `MATOMO_HOST`: URL to the Matomo host you want to report analytics to, with trailing /. Remove to disable. (example `https://stats.home.com/`). 
- `MATOMO_SITE`: ID of the Matomo website you want to report analytics (example `6`)
- `MATOMO_TAG_MANAGER_URL`: URL to the Matomo Tag Manager script, typically in the format https://<DOMAIN>/js/container_<ID>.js (either use this or MATOMO_HOST+MATOMO_SITE but not both)
- `REPOSITORY_URL`: URL of the software repository (example `https://github.com/osm-fr/podoma`)
- `VECT_STYLE` : URL to [Maplibre GL compatible style](https://maplibre.org/maplibre-gl-js/docs/) (example `https://tile-vect.openstreetmap.fr/styles/liberty/style.json`)
- `MAP_INITIAL_ZOOM` : Initial zoom level of the map
- `MAP_INITIAL_LAT` : Latitude of the initial center of the map
- `MAP_INITIAL_LON` : Longitude of the initial center of the map
- `PDM_TILES_URL`: URL to access the _pg_tileserv_ service, which provides the layers in your database
- `GEOJSON_BOUNDS`: object of `Geometry` type (polygon or multipolygon) in GeoJSON delimiting the area to search for OSM notes.
- `MAINTENANCE_MODE`: optional flag to display a notice page on website when doing maintenance (set to `true` to enable)

### Postgresql connection

As to connect to any Postgresql host, `DB_URL` environement variable is expected to be set with a conninfo string.
This is necessary for standalone or Docker environments.

```bash
export DB_URL="postgres://user:password@host:5432/database"
```

See also 33.1.1 chapter about [Postgresql conninfo strings](https://www.postgresql.org/docs/13/libpq-connect.html).

## Project configuration

Please see the dedicated documentation in PROJECTS.md

## Build

Once PDM has been properly configured, you should choose between Docker or standalone to build it.
Refers to Database section in run chapter to make Podoma fully runnable.

### git submodules

Podoma relies on some git submodules. Please mind using the following to retrieve them prior to build

```sh
git submodule init
git submodule update
```

### Docker build

You can build a node.js based Podoma server including necessary features to run. It includes osmium 1.10.0 with Debian Buster.
It doesn't includes a PgSQL server. You can use [CampToCamp Postgres image](https://hub.docker.com/r/camptocamp/postgres/tags?page=1&ordering=last_updated).

```bash
docker build [--build-arg IMPOSM3_VERSION=0.11.0] [--build-arg CONFIG=./config.json] -t pdm/server:latest .
```

Where:

- IMPOSM3_VERSION: Version of imposm3 to use in the docker image
- CONFIG : Path towards the config file to be used (defaults to `./config.json`)

### Standalone instance

```bash
npm install
```

## Run

### Database

The database relies on PostgreSQL. To create the database, as a superuser :

```bash
psql -c "CREATE DATABASE pdm"
psql -c "CREATE EXTENSION IF NOT EXISTS postgis"
psql -c "CREATE EXTENSION IF NOT EXISTS hstore"
```

### pg_tileserv

#### Without Docker

As pg_tileserv is required to display osm data on the map, you'll have to install it or use the image include in docker-compose file.
You will need `wget` and `unzip` to use it.

```bash
# download latest build
wget https://postgisftw.s3.amazonaws.com/pg_tileserv_latest_linux.zip
# unzip / delete it
unzip pg_tileserv_latest_linux.zip -d /opt/pg_tileserv
rm pg_tileserv_latest_linux.zip
# specify database url to connect
export DATABASE_URL=postgres://postgres:password@pdm-db:5432/pdm
# launch it
cd /opt/pg_tileserv
./pg_tileserv
```

#### Using Docker

You can use the image in docker-compose.yml or for a lightweight image you should build yourself the [alpine Dockerfile](https://github.com/CrunchyData/pg_tileserv/blob/master/Dockerfile.alpine)
It's possible to install pg_tileserv in pdm Dockerfile before building the image. But it's not recommended.

### Docker

Database is installed and inited simply with:

```bash
docker run --rm [--network=your-network] -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest install
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest init
```
Second command will init each project in the database. It can lead to significant process times depending on chosen perimeter.
And then run the server with:

```bash
docker run -d --rm [--network=your-network] -p 3000:3000 --name=pdm -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest run
```

Don't forget to add following lines into your crontab for periodic updates, for example daily updates:

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_daily
```

Individual updates are also available for punctual calls:

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_features
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_changes {init,update} [keep]
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_projects [init]s
```
See below for the related documentation.

If you use Imposm updates, you may start and let run another container that handles regular updates as well:

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_imposm
```

### Standalone

Database relies on PostgreSQL. To install the schema before first run:

```bash
psql -d pdm -f db/01_setup_schema.sql
```

The following script is to run to retreive and update changes:

```bash
npm run features:update
./db/11_features_update_tmp.sh
```
It supports two modes:
* init, downloads an OSM.pbf file and use imposm to import it in database
* update, to refresh materialized views.

The following script is to run after first initialization of database to create list of OSM features:

```bash
npm run features:update
./db/21_features_update_tmp.sh {init,update}
```
It supports two modes:
* init, to fill each project changelog on the whole period between their start and now
* update, to fill each project changelog on the whole period since their last update and now

The following script has to be launched daily to retrieve the contribution statistics (notes, objects added, badges obtained):

```bash
npm run projects:update
./db/31_projects_update_tmp.sh
```

### Disable imposm3 usage

Is it possible to not use imposm3 and provide a live updated database with all necessary data.  
You must ensure yourself it will be updated at the necessary pace for PdM need, with minute diffs.

Once setup, don't forget to set `database.live` to `true` in your project(s) configuration.

#### Common principles

All geometries imported by imposm are intended to be displayed in web tiles, so prefer using the EPSG:3857 projection. They differ from the ones stored in the changelog, mainly intended for project computations, thus with EPSG:4326.

A table `pdm_boundary` with every administrative boundaries will be useful to aggregate statistics at several levels. [Administrative boundaries](https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative) 2, 4, 6 and 8 are supported by web interface.  
It should conform to this structure:

```sql
id INT
osm_id BIGINT
name VARCHAR
admin_level INT
tags HSTORE
geom GEOMETRY(Geometry, 3857)
centre GEOMETRY(Point, 3857)
```

`centre` column contains a point that must be inside the boundary perimeter (please use [ST_PointOnSurface](https://postgis.net/docs/ST_PointOnSurface.html) function).  
Indices should be created on columns `osm_id`, `tags`, `geom` and `centre` depending on the amount of boundaries covering each project.

Processing will automatically derivate another table `pdm_boundary_subdivide` by using [ST_Subdivide](https://postgis.net/docs/ST_Subdivide.html) as to significantly ease the intersecting computation between features and boundaries.  
The only transformation from 3857 to 4326 is done on boundaries subdivide, since they are compared to changelog geometries for boundaries counts.

Once updated, use the following to propagate the changes you have made to the boundaries:

```sql
REFRESH MATERIALIZED VIEW pdm_boundary_subdivide;
```

#### Replace by another database
In case you have access to your own database already updated outside of PdM, you should produce a materialized view for each active project, called `pdm_project_${project_slug}` that conforms to following structure:

```sql
osm_id varchar,
gid bigint,
name VARCHAR(255)
tags json
geom GEOMETRY(Geometry, 3857)
```

Optionally, if the compare mode is enabled for a given project, a supplemntary view called `pdm_project_${project_slug}_compare` that conforms to the given structure for `pdm_project_${project_slug}` is needed.

#### Replace by the changelog
If you accept to only have a daily update to most statsitics, which means without instant update when some features get edited along the day, it is possible to create a materialized view as such:

```sql
create materialized view pdm_project_projectslug as
  select fc.osmid as osm_id, 
  split_part(fc.osmid, '/', 2)::bigint as gid, 
  fc.tags->>'name' as name, 
  to_json(fc.tags) as tags, 
  ST_Transform(fc.geom, 3857) as geom
  from pdm_features_projectslug_changes fc
  where fc.tagsfilter=true and (CURRENT_TIMESTAMP BETWEEN fc.ts_start AND fc.ts_end
        OR (CURRENT_TIMESTAMP > fc.ts_start AND fc.ts_end is null));
```

Please mind refreshing it every day.

## Website

The code for the web interface can be found in the `website` folder. This is an [ExpressJS](http://expressjs.com/) server, combined with [Pug](https://pugjs.org) templates.

The Pug templates are in the `templates` sub-folder. It is organized according to the following logic:

- In `templates`, the general model `layout.pug` and its CSS file
- In `common`, generic elements to all pages (`<head>`, header, footer)
- In `components`, the main components that populate the pages (map, statistics block...)
- In `pages`, each page of the site (home, map, project page...)

The site can be viewed at [localhost:3000](http://localhost:3000).

### Docker

Docker image includes websites and background updating tasks.
You can run it with:

```bash
docker run -d --rm [--network=your-network] -p 3000:3000 --name=pdm -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest run
```

### Docker compose

A compose file is provided to ease the running processing. It won't prevent you from creating database, adding users and make the appropriate configuration nor building dockers as mentionned upside.
Docker compose only allows to run easilly a functionnal instance if and only it has already been properly configured before.

Depending on your Postgresql configuration, you'll surely have to customize the `DB_URL` env variable in the compose file to let the pdm server access the database safely.

Don't try to run the instance with docker-compose first, try to run each component separately and check if everything work as expected.
Once everything runs normally, you can use the following for further runs:

To start:

```
docker-compose up
```

To stop:

```
docker-compose down
```

### Standalone

Standalone running requires a node server complient with compatility at the top of this document and planned tasks to update projects regularly.

To launch the web site :

```bash
export DB_URL="postgres://user:password@host:5432/database" # Database URL
export PORT=3000 # Nodejs port (defaults to 3000)
npm run start
```

The site can be viewed at [localhost:3000](http://localhost:3000).
