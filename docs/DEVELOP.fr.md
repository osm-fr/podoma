# Exploitation de Podoma

## Dépendances

- NodeJS >= 24
- Outils Bash : curl, mawk, grep, sed, bc
- PostgreSQL >= 15
- Python 3 (et le module `requests`)
- [Osmium](https://osmcode.org/osmium-tool/) > 1.18
- [osmctools](https://wiki.openstreetmap.org/wiki/Osmupdate)
- [Imposm](https://imposm.org/) >= 3
- [pg_tileserv](https://github.com/CrunchyData/pg_tileserv)
- Dépendances de [sendfile_osm_oauth_protector](https://github.com/geofabrik/sendfile_osm_oauth_protector#requirements)

### Compilation osmium

Podoma a besoin d'une version récente de osmium puisqu'il tire parti des nouvelles fonctions tags-filter.
Peu de distributions linux ont les dernières versions disponibles et vous aurez surement besoin de compiler osmium vous-même.

Voir les consignes sur le [README officiel](https://github.com/osmcode/osmium-tool/blob/master/README.md#building).

Les paquets suivants pourront être utiles pour cela

- build-essential
- cmake
- zlib1g-dev
- libbz2-dev
- liblz4-dev
- libboost-dev
- libboost-program-options-dev

## Débuter

```bash
git clone https://github.com/osm-fr/podoma.git
cd Podoma
git submodule update --init
```

## Configuration générale

La configuration générale de l'outil est à renseigner dans `config.json`. Un modèle est proposé dans le fichier `config.example.json`. Les paramètres sont les suivants :

- `WEBSITE_NAME`: nom visible du site web
- `WEBSITE_MOTTO`: slogan visible du site web
- `WEBSITE_URL`: adresse du site web (pour le SEO)
- `WEBSITE_DESCRIPTION`: description du site web (pour le SEO)
- `OSM_USER` : nom d'utilisateur OpenStreetMap pour la récupération de l'historique des modifications avec métadonnées
- `OSM_PASS` : mot de passe associé au compte utilisateur OSM
- `OSM_CLIENT_ID` : client ID généré depuis le compte OpenStreetMap
- `OSH_PBF_AUTHORIZED`: Active l'authentification par OAuth2 pour être autorisé à télécharger les fichiers PBF lorsque c'est nécessaire. A désactiver si le dépôt indiqué dans `OSH_PBF_URL` ne le supporte pas.
- `OSH_PBF_URL` : URL du fichier OSH.PBF (historique et métadonnées, exemple `https://osm-internal.download.geofabrik.de/europe/france/reunion-internal.osh.pbf`)
- `OSM_PBF_URL`: URL du fichier OSM.PBF (etat courant de la base, exemple `https://download.geofabrik.de/europe/france-latest.osm.pbf`). Ce fichier n'est pas concerné par le processus d'autorisation.
- `POLY_URL`: URL d'un fichier de polygone dans lequel les projets existent (exemple `https://download.geofabrik.de/europe/france.poly`) Ce fichier n'est pas concerné par le processus d'autorisation.
- `DB_USE_IMPOSM_UPDATE` : Active ou désactive l'intégration d'imposm3 (permet d'utiliser une base existante et tenue à jour par d'autres moyens, par défaut `true`)
- `WORK_DIR` : dossier de téléchargement et stockage temporaire (doit pouvoir contenir le fichier OSH PBF, exemple `/tmp/pdm`)
- `OSM_URL` : instance OpenStreetMap à utiliser (exemple `https://www.openstreetmap.org`)
- `OSM_API_URL` : instance API OpenStreetMap à utiliser (exemple `https://api.openstreetmap.org`)
- `JOSM_REMOTE_URL` : adresse du serveur JOSM à contacter (exemple `http://localhost:8111`)
- `OVERPASS_URL`: URL vers l'interpreter d'une instance Overpass API utilisée pour la récupération des membres manquants
- `OVERPASS_FATAL`: indique si une requête Overpass échouée doit interrompre `update_changes`. Si false, le script poursuivra le traitement des projets restants en omettant les membres manquants et seul un avertissement sera journalisé ; CELA INTRODUIRA DES ERREURS dans les statistiques, préférez si possible changer `OVERPASS_URL` pour une instance plus stable.
- `OSMOSE_URL` : instance Osmose à utiliser (exemple `https://osmose.openstreetmap.fr`)
- `NOMINATIM_URL` : instance de Nominatim à utiliser (exemple `https://nominatim.openstreetmap.org`)
- `MAPILLARY_URL` : instance Mapillary à utiliser (exemple `https://www.mapillary.com`)
- `MAPILLARY_API_KEY`: clé d'API Mapillary ([peut être créée ici](https://www.mapillary.com/dashboard/developers))
- `PANORAMAX_URL` : instance Panoramax à utiliser (exemple `https://explore.panoramax.fr`)
- `MATOMO_HOST`: URL vers l'hôte Matomo auquel nous souhaitons envoyer les statistiques terminant par un /. Supprimez pour désactiver. (exemple `https://stats.maison.fr/`)
- `MATOMO_SITE`: Identifiant du site web Matomo auquel se rapportent les statistiques (exemple `6`)
- `MATOMO_TAG_MANAGER_URL`: URL vers le script Matomo Tag Manager, généralement au format https://<DOMAINE>/js/container_<ID>.js (utilisez soit ce paramètre, soit MATOMO_HOST+MATOMO_SITE, mais pas les deux)
- `REPOSITORY_URL` : URL du dépôt du logiciel (exemple `https://github.com/osm-fr/podoma`)
- `VECT_STYLE` : URL d'un [style compatible avec Maplibre GL](https://maplibre.org/maplibre-gl-js/docs/) (exemple `https://tile-vect.openstreetmap.fr/styles/liberty/style.json`)
- `MAP_INITIAL_ZOOM` : Niveau de zoom initial de la carte
- `MAP_INITIAL_LAT` : Latitude du centre initial de la carte
- `MAP_INITIAL_LON` : Longitude du centre initial de la carte
- `PDM_TILES_URL` : URL d'accès au service _pg_tileserv_, qui met à disposition les couches dans votre base de données
- `GEOJSON_BOUNDS` : objet de type `Geometry` (polygone ou multipolygone) en GeoJSON délimitant la zone où rechercher des notes OSM
- `MAINTENANCE_MODE`: option pour afficher une page spéciale sur le site indiquant qu'il est en cours de maintenance (valeur `true` pour activer)

### Connexion à Postgresql

Pour se connecter à Postgresql, une variable d'environement `DB_URL` est attendue avec une chaine conninfo pour disposer des informations nécessaires.
Ceci est valable quelle que soit la solution retenue, docker ou locale.

```bash
export DB_URL="postgres://user:password@host:5432/database"
```

Reportez-vous au chapitre 33.1.1 du [guide postgresql](https://www.postgresql.org/docs/13/libpq-connect.html).

## Configuration des projets

Voir la documentation dédiée dans PROJECTS.md

## Installation

L'installation peut se faire en utilisant docker ou bien directement sur l'hôte.
Confère à la section Déploiement ci-dessous pour obtenir un Podoma exploitable.

### Submodules git

Podoma dépend de quelques sous-modules git. Pensez à executer les commandes suivantes avant le build :

```sh
git submodule init
git submodule update
```

### Construction Docker

Il est possible de construire un serveur node.js unique pourvu des fonctionnalités nécessaires à toute l'exploitation. Il inclus osmium 1.10.0 avec Debian Buster.
L'image Docker n'inclue cependant pas de serveur postgresql et vous pourrez utiliser [l'image de CampToCamp](https://hub.docker.com/r/camptocamp/postgres/tags?page=1&ordering=last_updated).

```bash
docker build [--build-arg IMPOSM3_VERSION=0.11.0] [--build-arg CONFIG=./config.json] -t pdm/server:latest .
```

Avec :

- IMPOSM3_VERSION : Version d'imposm3 à intégrer à l'image Docker
- CONFIG : Chemin vers le fichier de configuration principal à utiliser (Par défaut `./config.json`)

### Installation locale

Assurez-vous d'avoir une version de npm fonctionnelle avant de lancer la commande suivante :

```bash
npm install
```

## Déploiement

### Base de données

La base de données utilise PostgreSQL. Pour créer la base, lancez la commande en tant que super-utilisateur :

```bash
psql -c "CREATE DATABASE pdm"
psql -c "CREATE EXTENSION IF NOT EXISTS postgis"
psql -c "CREATE EXTENSION IF NOT EXISTS hstore"
```

### pg_tileserv

#### Sans Docker

pg_tileserv étant requit pour afficher la données osm sur la carte, vous pouvez soit l'installer soit utiliser une image Docker.
Pour l'installation `wget` et `unzip` sont nécessaires.

```bash
# télécharge la dernière version
wget https://postgisftw.s3.amazonaws.com/pg_tileserv_latest_linux.zip
# décompression / suprresion
unzip pg_tileserv_latest_linux.zip -d /opt/pg_tileserv
rm pg_tileserv_latest_linux.zip
# url de la base de données à laquelle se connecter
export DATABASE_URL=postgres://postgres:password@pdm-db:5432/pdm
# lanchement de pg_tileserver
cd /opt/pg_tileserv
./pg_tileserv
```

#### Avec Docker

Vous pouvez aussi utiliser l'image incluse dans le docker-compose.yml ou, pour une image plus lègere utiliser le [Dockerfile alpine suivant](https://github.com/CrunchyData/pg_tileserv/blob/master/Dockerfile.alpine) afin de build vous même l'image.
Vous pouvez aussi très bien installer pg_tileserv dans pdm via le Dockerfile pour builder l'image Docker pdm en l'incluant. Mais ceci n'est pas recommandé.

### Docker

Le service s'installe avec les deux commandes suivantes :

```bash
docker run --rm [--network=your-network] -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest install
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest init
```
La deuxième commande va initialiser chaque projet dans la base de données et peut donner lieu à des temps de traitement importants selon le périmètre configuré.

Ensuite, lancez le serveur avec la commande :

```bash
docker run -d --rm [--network=your-network] -p 3000:3000 --name=pdm -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest run
```

N'oubliez pas d'ajouter les lignes suivantes dans vos crontab pour mettre à jour les projets périodiquement, idéalement quotidiennement.

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_daily
```

Des commandes dédiées sont néamoins disponibles pour relancer une update particulière :

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_features
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_changes {init,update} [keep]
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_projects [init]
```

Si vous utilisez la mise à jour via Imposm, vous devez avoir un container qui tourne en fond pour gérer les mises à jour régulières :

```bash
docker run --rm [--network=your-network] -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest update_imposm
```

Voir ci-dessous pour la portée de chacune.

### Instance locale

La base de données s'appuie sur PostgreSQL. Pour installer la base :

```bash
psql -d pdm -f db/01_setup_schema.sql
```

Le script suivant est à lancer pour permettre l'import imposm :

```bash
npm run features:update
./db/11_features_update_tmp.sh {init,update}
```

Il comporte deux modes :
* init, télécharge un fichier OSM.pbf et lance un import complet imposm
* update, rafraichit des vues matérielles (probablement à supprimer)


Le script suivant permet de mettre à jour quotidiennement le journal des changes:  

```bash
npm run changes:update
./db/21_changes_update_tmp.sh {init, update}
```

Il comporte deux modes :
* init, initialise le journal des projets passés et en cours sur l'ensemble de la période de leur date de départ à aujourd'hui
* update, met à jour au fil de l'eau l'ensemble des projets depuis leur date de mise à jour respective

Le script suivant est à lancer quotidiennement pour récupérer les statistiques de contribution (notes, objets ajoutés, badges obtenus) :

```bash
npm run projects:update
./db/31_projects_update_tmp.sh
```

### Se passer d'imposm3

Il est possible de ne pas utiliser imposm3 et de se connecter à une base de données pourvue des données nécessaires.
Il faudra s'assurer qu'elle est tenue à jour toutes les heures minimum pour les besoins de PdM.

Une fois configuré, n'oubliez pas de définir `database.live` à `true` dans la configuration de votre projet.

#### Généralités

Toutes les géométries importées par imposm sont prévues pour être affichées sur des tuiles web, la projection EPSG:3857 est alors préférée. Ces géométries diffèrent de celles stockées pour le changelog, principalement destiné aux calculs sur les projets, ainsi en EPSG:4326.

Il est nécessaire d'avoir une table `pdm_boundary` contenant le découpage administratif de la zone ([niveaux administratifs](https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative) 2, 4, 6 et 8) et ayant cette structure :

```sql
id INT
osm_id BIGINT
name VARCHAR
admin_level INT
tags HSTORE
geom GEOMETRY(Geometry, 3857)
centre GEOMETRY(Point, 3857)
```

La colonne `centre` est comprise comme étant un point compris dans le périmètre de la limite (vous pouvez utiliser [ST_PointOnSurface](https://postgis.net/docs/ST_PointOnSurface.html)).

Créer des indexes sur les colonnes `osm_id`, `tags`, `geom` et `centre` peut être utile suivant la population d'objets touchée par un projet donné.

PdM va automatiquement créer une table `pdm_boundary_subdivide` en utilisant [ST_Subdivide](https://postgis.net/docs/ST_Subdivide.html) pour faciliter le calcul d'intersection entre les objets du projet et le zonage administratif.  
La seule transformation du système 3857 vers 4326 a lieu lors de la subdivision des périmètres administratifs, pour les comparer aux géométries du changelog pour les calculs localisés sur ces périmètres.

Une fois mise à jour, pensez à propager les changements aux vues utilisants les périmètres administratifs :

```sql
REFRESH MATERIALIZED VIEW pdm_boundary_subdivide;
```

#### Remplacé par une autre base de données
Dans le cas vous disposez de votre propre base de données tenue à jour en dehors de Podoma, il faudra produire des vues matérialisées pour chaque projet configurés appelées `pdm_project_${project_slug}`, avec la structure suivante :

```sql
osm_id varchar,
gid bigint,
name VARCHAR(255)
tags json
geom GEOMETRY(Geometry, 3857)
```

Optionellement, si le mode compare est activé dans un projet donné, une vue supplémentaire appelée `pdm_project_${project_slug}_compare` conforme à ce qui doit être comparé est nécessaire. Elle a la même structure que ci-dessus.

#### Remplacé par le journal des modifications
Dans le cas où vous accepteriez une mise à jour quotidienne de la vue des objets actuels, c'est à dire sans prise en compte immédiate des objets contribués pendant la journée, il est possible de créer manuellement une vue matérialisée comme suit :

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

Il faut penser à la mettre à jour chaque jour.

## Site web

Le code de l'interface web se trouve dans le dossier `website`. Il s'agit d'un serveur [ExpressJS](http://expressjs.com/), combiné à des modèles [Pug](https://pugjs.org).

Les modèles Pug sont dans le sous-dossier `templates`. Celui-ci est organisé selon la logique suivante :

- Dans `templates`, le modèle général `layout.pug` et son fichier CSS
- Dans `common`, les éléments génériques à toutes les pages (`<head>`, en-tête, pied de page)
- Dans `components`, les composants principaux qui peuplent les pages (carte, bloc statistiques...)
- Dans `pages`, chacune des pages du site (accueil, carte, page projet...)

Le site est visible à l'adresse [localhost:3000](http://localhost:3000).

### Docker

L'image Docker inclue le nécessaire pour servir le site web et executer les tâches de mise à jour automatiques.
Vous pouvez la lancer avec:

```bash
docker run -d --rm [--network=your-network] -p 3000:3000 --name=pdm -v host_work_dir:container_work_dir -e DB_URL=postgres://user:password@host:5432/database pdm/server:latest run
```

### Docker compose

Un fichier docker-compose est fourni ici pour faciliter l'execution de la plateforme. Cela ne vous dispensera pas de créer la base de données, y ajouter les bons rôles et configurer pdm de manière appropriée selon la méthode expliquée ci-dessus.
Docker-compose permet seulement de faciliter l'execution d'une instance déjà fonctionnelle si et seulement si elle a été configurée correctement préalablement.

En fonction de la configuration de votre serveur Postgresql, vous devrez certainement adapter la valeur de la variable `DB_URL` dans le fichier compose pour permettre au serveur pdm d'accéder à la base de données correctement.

N'essayez pas de débuter la configuration d'une instance avec docker-compose, essayez plutôt d'obtenir une configuration fonctionnelle de chaque composant et de vous assurer que tout fonctionne séparément d'abord.
Une fois que vous avez constaté que tout fonctionnait, lancez-vous avec docker-compose pour faciliter les executions futures.

Pour démarrer :

```
docker-compose up
```

Pour arrêter :

```
docker-compose down
```

### Instance locale

L'execution locale nécessite un serveur node conforme à la compatilité ci-dessus et des tâches planifiées pour tenir la base de données à jour.

Pour lancer le site web :

```bash
export DB_URL="postgres://user:password@host:5432/database" # Database URL
export PORT=3000 # Nodejs port (defaults to 3000)
npm run start
```

Le site est accessible à cette url [localhost:3000](http://localhost:3000).
