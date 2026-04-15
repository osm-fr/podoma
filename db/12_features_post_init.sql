-- Boundary subdivide
CREATE MATERIALIZED VIEW pdm_boundary_subdivide AS
SELECT id, osm_id, name, admin_level, tags, ST_Subdivide(ST_Transform(geom, 4326), 450) AS geom
FROM pdm_boundary;

CREATE INDEX ON pdm_boundary_subdivide using gist(geom);
CREATE INDEX ON pdm_boundary_subdivide using btree(osm_id);

-- Boundaries common statistics
CREATE MATERIALIZED VIEW pdm_boundary_stats AS
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
		COALESCE(lag(fb.amount) over project_window, 0) as amount_prev,
		d.ts,
		b.name,
		b.admin_level,
		b.geom,
		COALESCE(last_value(fb.amount) over project_window, 0) - COALESCE(first_value(fb.amount) over project_window, 0) as delta_project,
		COALESCE(fb.amount, 0) - COALESCE(lag(fb.amount) over project_window, 0) as delta_prev
	FROM pdm_counts_dates d
	JOIN boundaries b ON b.project_id=d.project_id
	LEFT JOIN pdm_feature_counts_per_boundary fb ON fb.project_id=d.project_id AND fb.ts=d.ts AND fb.boundary=b.boundary
	WHERE fb.label IS NULL
	WINDOW project_window AS (PARTITION BY d.project_id, b.boundary ORDER BY d.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT
	s.boundary as boundary,
	s.project_id,
	s.label,
	s.name,
	s.admin_level,
	s.geom,
	json_object(array_agg(s.ts::date::text), array_agg(s.amount::text)) AS amounts_json,
	json_object(array_agg(s.ts::date::text), array_agg(s.delta_prev::text)) AS deltas_json,
	array_agg(s.delta_prev) AS deltas,
	(array_agg(s.delta_prev))[array_upper(array_agg(s.delta_prev), 1)] as delta_daily,
	s.delta_project
FROM stats s
GROUP BY s.project_id, s.label, s.boundary, s.delta_project, s.name, s.admin_level, s.geom;

CREATE INDEX pdm_boundary_stats_project_idx ON pdm_boundary_stats(project_id);
CREATE INDEX pdm_boundary_stats_admin_idx ON pdm_boundary_stats(admin_level);
CREATE INDEX pdm_boundary_stats_label_idx ON pdm_boundary_stats(label);

-- Boundaries statistics for tiles
CREATE MATERIALIZED VIEW pdm_boundary_tiles AS
SELECT
	s.boundary as id,
	s.boundary as boundary,
	s.project_id,
	s.label,
	s.name,
	s.admin_level,
	s.geom,
	s.amounts_json as stats,
	s.delta_project,
	s.delta_daily
FROM pdm_boundary_stats s WHERE s.label IS NULL;

CREATE INDEX pdm_boundary_tiles_project_idx ON pdm_boundary_tiles(project_id);
CREATE INDEX pdm_boundary_tiles_geom_idx ON pdm_boundary_tiles USING GIST(geom);

-- Boundaries statistics for dashboard
-- Dashboard is built on 20 or fewer last days of contribution. Typical contribution ranges for each admin_level come from 1st and 4th quartiles
CREATE MATERIALIZED VIEW pdm_boundary_dash AS
WITH delta_length AS (
  -- For each project, get the most continuous and recent period of daily deltas in the counts list
  SELECT
	project_id,
	first_value(ts) over ts_window as ts,
	least(20, first_value(counter) over ts_window) as counter
  FROM pdm_counts_dates_deltas
  WHERE delta=interval '1 day'
  WINDOW ts_window as (PARTITION BY project_id order by ts desc)
),
delta_rank AS (
  -- For each label, admin_level of each project, ranking each delta in ascending order
  SELECT
    s.project_id,
    s.admin_level,
    s.label,
    unnest(s.deltas[array_length(s.deltas, 1)-dl.counter:array_length(s.deltas, 1)]) as delta,
    PERCENT_RANK() OVER last_window as percentile_rank
  FROM pdm_boundary_stats s
  JOIN delta_length dl ON dl.project_id=s.project_id
  WINDOW last_window AS (PARTITION BY s.project_id, s.admin_level, s.label ORDER BY unnest(s.deltas[array_length(s.deltas, 1)-dl.counter:array_length(s.deltas, 1)]) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
bottom_10 AS (
  -- For each label, admin_level of each project, fiding the least quartile of delta
  SELECT project_id, admin_level, label, round(AVG(delta)) as delta
  FROM delta_rank
  WHERE percentile_rank <= 0.25
  GROUP BY project_id, admin_level, label
),
top_10 AS (
  -- For each label, admin_level of each project, fiding the greatest quartile of delta
  SELECT project_id, admin_level, label, round(avg(delta)) as delta
  FROM delta_rank
  WHERE percentile_rank >= 0.75
  GROUP BY project_id, admin_level, label
),
boundaries_activity as (
  -- For each label, admin_level of each project, fiding the least and most active boundaries
  SELECT DISTINCT
    s.project_id,
    s.admin_level,
    s.label,
	first_value(s.delta_project) over full_window as delta_project_min,
	last_value(s.delta_project) over full_window as delta_project_max,
    first_value(s.boundary) over full_window as boundary_min,
    last_value(s.boundary) over full_window as boundary_max
  FROM pdm_boundary_stats s
  WINDOW full_window AS (PARTITION BY s.project_id, s.admin_level, s.label ORDER BY s.delta_project ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
),
admlevel_stats as (
  -- For each label, admin_level of each project, combining the quartiles to get the daily average contributing range
  SELECT
    b.project_id,
    b.admin_level,
    b.label,
    min(b.delta) as delta_daily_min,
    max(t.delta) as delta_daily_max
    FROM bottom_10 b
    JOIN top_10 t ON t.project_id=b.project_id AND t.admin_level=b.admin_level AND coalesce(t.label,'_global')=coalesce(b.label,'_global')
    GROUP BY b.project_id, b.admin_level, b.label
)
-- Finally combining daily average contributing range and least/most active areas to get the boundaries dashboard data
SELECT 
    a.project_id,
    a.admin_level,
    a.label,
    b.delta_project_min,
    b.delta_project_max,
    a.delta_daily_min,
    a.delta_daily_max,
    b.boundary_min,
    b.boundary_max
FROM admlevel_stats a
JOIN boundaries_activity b ON b.project_id=a.project_id AND b.admin_level=a.admin_level AND coalesce(b.label,'_global')=coalesce(a.label,'_global');

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
			t.boundary, t.name, t.admin_level, t.stats, t.delta_project, t.delta_daily
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
