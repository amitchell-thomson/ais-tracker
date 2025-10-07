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
	$(psql) -f db/seed-flow-role.sql

seed-flow-roles:
	$(psql) -f db/seed-flow-role.sql

caggs-mvs:
	$(psql) -f features/caggs_and_mvs.sql

features:
	$(psql) -f features/ml_features.sql

setup: init seed-areas caggs-mvs features

maintenance:
	$(psql) -f features/maintenance.sql

