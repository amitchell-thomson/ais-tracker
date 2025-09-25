\set ON_ERROR_STOP on
\echo CWD:
\! pwd

BEGIN;

-- 0) Adjust this path if your file lives elsewhere (use forward slashes on Windows too)
-- \set _feature_path 'C:/Users/alecm/OneDrive/Desktop/ais-tracker/db/ais-areas.geojson'

-- 1) Load the file line-by-line (works for multi-line JSON)
DROP TABLE IF EXISTS _raw;
CREATE TEMP TABLE _raw(line text);
\copy _raw(line) FROM 'db/ais-areas.geojson'

-- 2) Recombine lines into a single JSONB document
DROP TABLE IF EXISTS _fc;
CREATE TEMP TABLE _fc(doc jsonb);
INSERT INTO _fc(doc)
SELECT string_agg(line, E'\n')::jsonb FROM _raw;

-- 3) Expand to features with extracted properties + geometry (Polygon)
DROP TABLE IF EXISTS _features;
CREATE TEMP TABLE _features AS
WITH f AS (
  SELECT jsonb_array_elements(doc->'features') AS feat
  FROM _fc
)
SELECT
  trim(feat->'properties'->>'name')             AS name,
  trim(feat->'properties'->>'kind')             AS kind,        -- port | lane | chokepoint | sts
  trim(feat->'properties'->>'subtype')          AS subtype,     -- core | approach | corridor | gate_*
  NULLIF(trim(feat->'properties'->>'group'),'') AS "group",
  NULLIF(trim(feat->'properties'->>'notes'),'') AS notes,
  ST_SetSRID(
    ST_GeomFromGeoJSON((feat->'geometry')::text),
    4326
  )::geometry(Polygon,4326) AS geom,
  (trim(feat->'properties'->>'subtype') LIKE 'gate_%') AS is_gate
FROM f;

-- 4) Use human-readable & unique area_id composed of name/kind/subtype
DROP TABLE IF EXISTS _area_rows;
CREATE TEMP TABLE _area_rows AS
SELECT
  (trim(name) || ' // ' || trim(kind)) AS area_id,
  name, kind, subtype, "group", notes, geom
FROM _features
WHERE NOT is_gate;

-- (Gate IDs: human-readable, same composition)
DROP TABLE IF EXISTS _gate_rows;
CREATE TEMP TABLE _gate_rows AS
SELECT
  (trim(name) || ' // ' || trim(kind)) AS gate_id,
  name, kind, subtype, "group", notes, geom
FROM _features
WHERE is_gate;

-- 5) Upsert AREAS (dedupe by area_id within this INSERT)
INSERT INTO public.area (area_id, name, kind, subtype, "group", notes, geom)
SELECT DISTINCT ON (area_id)
       area_id, name, kind, subtype, "group", notes, geom
FROM _area_rows
ORDER BY area_id
ON CONFLICT (area_id) DO UPDATE
SET name    = EXCLUDED.name,
    kind    = EXCLUDED.kind,
    subtype = EXCLUDED.subtype,
    "group" = EXCLUDED."group",
    notes   = EXCLUDED.notes,
    geom    = EXCLUDED.geom;

-- 6) Link GATES to parent corridor/chokepoint (parent_area_id = area.area_id)
DROP TABLE IF EXISTS _gate_join;
CREATE TEMP TABLE _gate_join AS
SELECT
  g.gate_id, g.name, g.kind, g.subtype, g."group", g.notes, g.geom,
  a.area_id AS parent_area_id
FROM _gate_rows g
LEFT JOIN public.area a
  ON a.kind = g.kind
 AND a.subtype = 'corridor'
 AND a."group" IS NOT DISTINCT FROM g."group";

INSERT INTO public.area_gate (gate_id, area_id, name, kind, subtype, "group", notes, geom)
SELECT DISTINCT ON (gate_id)
       gate_id, parent_area_id, name, kind, subtype, "group", notes, geom
FROM _gate_join
WHERE parent_area_id IS NOT NULL
ORDER BY gate_id
ON CONFLICT (gate_id) DO UPDATE
SET area_id = EXCLUDED.area_id,
    name    = EXCLUDED.name,
    kind    = EXCLUDED.kind,
    subtype = EXCLUDED.subtype,
    "group" = EXCLUDED."group",
    notes   = EXCLUDED.notes,
    geom    = EXCLUDED.geom;

COMMIT;

-- Summary (optional)
SELECT 'areas_inserted'   AS what, count(*) AS num FROM _area_rows;
SELECT 'gates_total'      AS what, count(*) AS num FROM _gate_rows;
SELECT 'gates_linked'     AS what, count(*) AS num FROM _gate_join WHERE parent_area_id IS NOT NULL;
SELECT 'gates_unmatched'  AS what, count(*) AS num FROM _gate_join WHERE parent_area_id IS NULL;
