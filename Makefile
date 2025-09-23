DB=ais
USER=ais
PASS=aispass
HOST=localhost
PORT=5432

psql = PGPASSWORD=$(PASS) psql -h $(HOST) -p $(PORT) -U $(USER) -d $(DB)

init:
	$(psql) -f db/init.sql

seed-areas:
	$(psql) -f db/seed-areas-from-geojson.sql

seed-flow-roles:
	$(psql) -f db/seed-flow-roles.sql

caggs:
	$(psql) -f db/caggs.sql

ewm:
	$(psql) -f db/ewm_views.sql

features:
	$(psql) -f db/features.sql