#!/bin/sh

# copy those 3 lines as many times as the csv files that are going to be unpivoted and rename the variable names accordingly
# and then go to the Unpivotig Section and do the same.
unpivot_csv_1=<csv file to be unpivoted>
selected_columns_1=<txt file with the columns of the initial csv that will be included in the unpivoted csv> 
unpivot_columns_1=<txt file with the columns of the initial csv that will be unpivoted in the unpivoted csv> 



dockerize -template /opt/postgresdb.properties/mipmap-db.properties.tmpl:/opt/postgresdb.properties/mipmap-db.properties
dockerize -template /opt/postgresdb.properties/i2b2-db.properties.tmpl:/opt/postgresdb.properties/i2b2-db.properties



# Unpivoting Section
echo "Unpivoting $unpivot_csv_1"

java -jar /opt/MIPMapReduced.jar -unpivot /opt/source/${unpivot_csv_1} /opt/postgresdb.properties/mipmap-db.properties "Attribute" /opt/map/${selected_columns_1} -u /opt/map/${unpivot_columns_1}


# Encounter and Patient Mapping Section
echo "Generating patient_num and encounter_num"

java -jar /opt/MIPMapReduced.jar -generate_id /opt/map/patientmapping.properties

java -jar /opt/MIPMapReduced.jar -generate_id /opt/map/encountermapping.properties

