#!/bin/sh

dockerize -template /opt/map/imaging_map.xml.tmpl:/opt/map.xml
dockerize -template /opt/postgresdb.properties/mipmap-db.properties.tmpl:/opt/postgresdb.properties/mipmap-db.properties
dockerize -template /opt/postgresdb.properties/i2b2-db.properties.tmpl:/opt/postgresdb.properties/i2b2-db.properties




echo 'Unpivoting imaging_data.csv'
java -jar /opt/MIPMapReduced.jar -unpivot /opt/source/volumes.csv /opt/postgresdb.properties/mipmap-db.properties "attribute" /opt/map/selected_volumes.txt -u /opt/map/unpivot_volumes.txt


echo 'generating patient_num and encounter_num'
java -jar /opt/MIPMapReduced.jar -generate_id /opt/map/patientmapping.properties

java -jar /opt/MIPMapReduced.jar -generate_id /opt/map/encountermapping.properties

echo 'Performing imaging capture mapping task' 

java -jar /opt/MIPMapReduced.jar /opt/map.xml /opt/postgresdb.properties/mipmap-db.properties -db /opt/postgresdb.properties/i2b2-db.properties	
