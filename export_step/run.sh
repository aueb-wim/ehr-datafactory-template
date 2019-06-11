#!/bin/sh

dockerize -template /opt/postgresdb.properties/i2b2-db-harmonized.properties.tmpl:/opt/postgresdb.properties/i2b2-db-harmonized.properties


# Mapping Section
echo "Exporting the flatten csv...."
 
java -jar MIPMapReduced.jar -runsql /opt/map/pivot_i2b2_cde_SF_MinDate_NEW111.sql /opt/postgresdb.properties/i2b2-db-harmonized.properties

echo "done!"
