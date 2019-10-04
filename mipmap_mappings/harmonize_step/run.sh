#!/bin/sh
mapping_xml_template=mapHarmonize.xml.tmpl


dockerize -template /opt/map/${mapping_xml_template}:/opt/map.xml
dockerize -template /opt/postgresdb.properties/mipmap-db.properties.tmpl:/opt/postgresdb.properties/mipmap-db.properties
dockerize -template /opt/postgresdb.properties/i2b2-db-harmonized.properties.tmpl:/opt/postgresdb.properties/i2b2-db-harmonized.properties


# Mapping Section
echo "Performing mapping task"
 
java -jar /opt/MIPMapReduced.jar /opt/map.xml /opt/postgresdb.properties/mipmap-db.properties -db /opt/postgresdb.properties/i2b2-db-harmonized.properties
