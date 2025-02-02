
CREATE EXTENSION h3;

CREATE EXTENSION h3_postgis CASCADE;

ALTER TABLE dscrtp ADD COLUMN geom geometry(Point, 4326) # 4326 is WGS84


UPDATE dscrtp SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)


ALTER TABLE dscrtp ADD COLUMN h3hex6