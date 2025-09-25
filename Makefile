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
	$(psql) -f db/seed-flow-role.sql

caggs-ewms:
	$(psql) -f features/caggs_and_ewms.sql

features:
	$(psql) -f features/ml_features.sql
