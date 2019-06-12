#!/bin/bash

mkdir -p output
chmod 777 output

# Postgres container info 
postgres_name="demo_postgres" # <enter the postgres container on hospital server>
i2b2_name="i2b2_harmonized"  # <harmonized i2b2 database name>
db_user="postgres"    # <postgres user>

# Mipmap paths 
mipmap_source="./source"
mipmap_pgproperties="./postgresdb.properties"
mipmap_target="./target"
mipmap_output="./output"
mipmap_preprocess="./preprocess_step"
mipmap_capture="./capture_step"
mipmap_harmonize="./harmonize_step"
mipmap_export="./export_step"


# Settings for exporting
export_csv="harmonized_clinical_data.csv"
pivoting_sql="pivot_i2b2_MinDate_NEW19_a.sql"

if [ -z $1 ]; then
    echo "EHR DataFactory step not declared. Exiting..." 
    exit 1
elif [ $1 = "capture" ]; then
    mipmap_map=$mipmap_capture
    use_mipmap=true
elif [ $1 = "harmonize" ]; then
    mipmap_map=$mipmap_harmonize
    use_mipmap=true
elif [ $1 = "preprocess" ]; then
    mipmap_map=$mipmap_preprocess
    use_mipmap=true
elif [ $1 = "export" ]; then
    mipmap_map=$mipmap_export
    use_mipmap=false
    echo "not using mipmap"
else
    echo "Not a EHR DataFactory step. Exiting..."
    exit 1
fi

# setting mipmap paths as enviroment variables to the host
export mipmap_map
export mipmap_source
export mipmap_pgproperties
export mipmap_target

if [ "$use_mipmap" = true ]; then
    echo "Performing EHR DataFactory $1 step"
    echo "Using $mipmap_map folder"
    if [ "$(docker ps -a -f name=^mipmap$)" ]; then
        echo "Using existing mipmap_etl container"
        docker-compose run mipmap_etl
    else 
        docker-compose up mipmap_etl
    fi
else
    if [ $1 = "export" ]; then
        echo "Performing EHR DataFactory $1 step"
        echo "Using $mipmap_map folder"
        # check if there is an existing flattened csv in postgres container and delete it
        if $(docker exec -i $postgres_name bash -c "[ -f /tmp/${export_csv} ]"); then
            echo "Deleting previous created export csv file..."
            docker exec $postgres_name rm -rf /tmp/${export_csv}
        fi
        # Copy the sql script to the postgres container
        docker exec -i $postgres_name sh -c "cat > /tmp/pivot_i2b2.sql" < ${mipmap_export}/${pivoting_sql}
        # run the sql script
        docker exec -it $postgres_name psql -q -U $db_user -d $i2b2_name -f /tmp/pivot_i2b2.sql
        # check if the flattened csv has been created
        if $(docker exec -i $postgres_name bash -c "[ -f /tmp/${export_csv} ]"); then
            # copy the flatten csv to the Data Factory output folder
            docker cp ${postgres_name}:/tmp/${export_csv} ${mipmap_output}/${export_csv}
            echo "Export csv created!"
        else
            echo "Error! The export csv was not created!"
            exit 1
        fi
    fi
fi
