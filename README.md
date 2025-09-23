# AIS Tracker
## Using AIS tanker positions to precict Brent vs WTI spread

PGPASSWORD=aispass psql -h localhost -p 5432 -U ais -d ais -f db/init.sql
