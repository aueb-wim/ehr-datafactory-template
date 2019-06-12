#!/bin/bash

mkdir -p anonym_output
chmod 777 anonym_output

# Postgres container info 
postgres_name="demo_postgres" # <enter the postgres container on hospital server>
db_harmonized="i2b2_harmonized" # < <harmonized i2b2 database name>
db_anonym="i2b2_anonymized"   # <harmonized i2b2 database name>
db_user="postgres"    # <postgres user>

# Mipmap paths 
mipmap_pgproperties="./postgresdb.properties"
mipmap_target="./target"
mipmap_output="./output"
mipmap_preprocess="./preprocess_step"
mipmap_capture="./capture_step"
mipmap_harmonize="./harmonize_step"
mipmap_export="./export_step"

# Settings for anonymization
mipmap_map=$mipmap_export
anonymized_csv="harmonized_clinical_anon_data.csv"
mipmap_anonym_output="./anonym_output"
anonymization_sql="md5_anonymize_db_FR_ides.sql"
pivoting_sql="pivot_i2b2_MinDate_NEW19_a_fed.sql"

if [ -z $1 ]; then
    echo "Error! Argument not given! Exiting... "
    exit 1
elif [ $1 = "i2b2" ]; then
    # make a copy of the harmonized database that will be anonymized later on. delete any previous anonymized db
    docker exec -it $postgres_name psql -U $db_user -d postgres -c "DROP DATABASE IF EXISTS $db_anonym;"
    docker exec -it $postgres_name psql -U $db_user -d postgres -c "CREATE DATABASE $db_anonym WITH TEMPLATE $db_harmonized;"

    # Copy the sql script to the postgres container
    docker exec -i $postgres_name sh -c "cat > /tmp/anonymize_i2b2.sql" < ${mipmap_export}/${anonymization_sql}
    # run the sql script
    docker exec -it $postgres_name psql -q -U $db_user -d $db_anonym -f /tmp/anonymize_i2b2.sql

elif [ $1 = "export" ]; then
    # EXPORTING DUMB ANONYMIZED DB - need additions
    # dump the anonymized db 
    echo "Dumbing $db_anonym ..."
    docker exec -t $postgres_name bash -c "pg_dump -U $db_user -F t $db_anonym > /tmp/anonymized.dump"
    docker cp ${postgres_name}:/tmp/anonymized.dump ${mipmap_anonym_output}/anonymized_db.dump
    echo "$db_anonym dumb file created in ${mipmap_anonym_output}/anonymized_db.dump"

    # EXPORTING AN ANONYMIZED FLAT CSV 
    echo "Exporting anonymized pivoted csv..."
    # check if there is an existing flattened csv in postgres container and delete it
    if $(docker exec -i $postgres_name bash -c "[ -f /tmp/$anonymized_csv ]"); then
        echo "Deleting previous created export csv file..."
        docker exec $postgres_name rm -rf /tmp/${anonymized_csv}
    fi
    # Copy the sql script to the postgres container
    docker exec -i $postgres_name sh -c "cat > /tmp/pivot_i2b2.sql" < ${mipmap_export}/$pivoting_sql
    # run the sql script
    docker exec -it $postgres_name psql -q -U $db_user -d $db_anonym -f /tmp/pivot_i2b2.sql
    # check if the flattened csv has been created
    if $(docker exec -i $postgres_name bash -c "[ -f /tmp/$anonymized_csv ]"); then
        # copy the flatten csv to the Data Factory output folder
        docker cp ${postgres_name}:/tmp/${anonymized_csv} ${mipmap_anonym_output}/${anonymized_csv}
        echo "Anonymized csv created in ${mipmap_anonym_output}/${anonymized_csv}"
    else
        echo "Error! The anonymized csv was not created!"
        exit 1
    fi
else
    echo "Error! Not valid argument given! Exiting...."
    exit 1
fi

# delete anonymized db
# echo "Dropping $db_anonym"
# docker exec -it $postgres_name psql -U $db_user -d postgres -c "DROP DATABASE IF EXISTS $db_anonym;"