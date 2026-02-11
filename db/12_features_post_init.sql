-- Boundary subdivide
CREATE MATERIALIZED VIEW pdm_boundary_subdivide AS
SELECT id, osm_id, name, admin_level, tags, ST_Subdivide(ST_Transform(geom, 4326), 450) AS geom
FROM pdm_boundary;

CREATE INDEX ON pdm_boundary_subdivide using gist(geom);
CREATE INDEX ON pdm_boundary_subdivide using btree(osm_id);

-- Boundary stats for tiles
CREATE MATERIALIZED VIEW pdm_boundary_tiles AS
WITH boundaries as (
	SELECT DISTINCT fb.project_id, fb.boundary, b.name, b.admin_level, b.centre as geom
	FROM pdm_feature_counts_per_boundary fb
	JOIN pdm_boundary b ON b.osm_id=fb.boundary
),
stats as (
	SELECT 
		b.boundary,
		d.project_id,
		NULL as label,
		COALESCE(fb.amount, 0) as amount,
		d.ts,
		b.name,
		b.admin_level,
		b.geom,
		COALESCE(last_value(fb.amount) over project_window, 0) - COALESCE(first_value(fb.amount) over project_window, 0) as nb
	FROM pdm_counts_dates d
	JOIN boundaries b ON b.project_id=d.project_id
	LEFT JOIN pdm_feature_counts_per_boundary fb ON fb.project_id=d.project_id AND fb.ts=d.ts AND fb.boundary=b.boundary
	WHERE fb.label IS NULL
	WINDOW project_window AS (PARTITION BY d.project_id, b.boundary ORDER BY d.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT
	s.boundary as id,
	s.boundary as boundary,
	s.project_id,
	s.label,
	s.name,
	s.admin_level,
	s.geom,
	json_object(array_agg(s.ts::date::text), array_agg(s.amount::text)) AS stats,
	s.nb
FROM stats s
GROUP BY s.project_id, s.label, s.boundary, s.nb, s.name, s.admin_level, s.geom;

CREATE INDEX pdm_boundary_tiles_project_idx ON pdm_boundary_tiles(project_id);
CREATE INDEX pdm_boundary_tiles_geom_idx ON pdm_boundary_tiles USING GIST(geom);

-- Filtered tiles function for pg_tileserv
CREATE OR REPLACE FUNCTION pdm_boundary_project_tiles(z integer, x integer, y integer, prjid int) RETURNS bytea AS $$
DECLARE
	result bytea;
BEGIN
	WITH bounds AS (
		SELECT ST_TileEnvelope(z, x, y) AS geom
	),
	mvtgeom AS (
		SELECT
			ST_AsMVTGeom(t.geom, bounds.geom) AS geom,
			t.boundary, t.name, t.admin_level, t.stats, t.nb
		FROM pdm_boundary_tiles t, bounds
		WHERE
			t.project_id = prjid
			AND t.label IS NULL
			AND CASE
				WHEN z < 5 THEN t.admin_level <= 4
				WHEN z >= 5 AND z < 8 THEN t.admin_level = 6
				WHEN z >= 8 THEN t.admin_level = 8
			END
			AND ST_Intersects(t.geom, bounds.geom)
	)
	SELECT ST_AsMVT(mvtgeom, 'public.pdm_boundary_project_tiles') INTO result
	FROM mvtgeom;

	RETURN result;
END;
$$
LANGUAGE 'plpgsql' STABLE PARALLEL SAFE;
