#!/bin/bash

mkdir -p output
chmod 777 output

postgres_name=demo_postgres
i2b2_name=i2b2_harmonized
db_user=postgres



if [ -z $1 ]; then
    echo "EHR DataFactory step not declared. Exiting..." 
    exit 1
fi
if [ $1 = "capture" ]; then
    mipmap_map="./capture_step"
    use_mipmap=true
else
    if [ $1 = "harmonize" ]; then
        mipmap_map="./harmonize_step"
        use_mipmap=true
    else
        if [ $1 = "preprocess" ]; then
            mipmap_map="./preprocess_step"
            use_mipmap=true
        else
            if [ $1 = "export" ]; then
                mipmap_map="./export_step"
                use_mipmap=false
                echo "not using mipmap"
            else
                echo "Not a EHR DataFactory step. Exiting..."
                exit 1
            fi
        fi
    fi
fi


mipmap_source="./source"
mipmap_pgproperties="./postgresdb.properties"
mipmap_target="./target"
mipmap_output="./output"
export mipmap_map
export mipmap_source
export mipmap_pgproperties
export mipmap_target

if [ "$use_mipmap" ]; then
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
        # check if there is an existing flattened csv in postgres container and delete it
        if $(docker exec -i $postgres_name bash -c "[ -f /tmp/harmonized_clinical_data.csv ]"); then
            echo "Deleting previous created export csv file..."
            docker exec $postgres_name rm -rf /tmp/harmonized_clinical_data.csv
        fi
        # Copy the sql script to the postgres container
        docker exec -i $postgres_name sh -c "cat > /tmp/pivot_i2b2.sql" < ./export_step/pivot_i2b2_MinDate_NEW19_a.sql
        # run the sql script
        docker exec -it $postgres_name psql -q -U $db_user -d $i2b2_name -f /tmp/pivot_i2b2.sql
        # check if the flattened csv has been created
        if $(docker exec -i $postgres_name bash -c "[ -f /tmp/harmonized_clinical_data.csv ]"); then
            # copy the flatten csv to the Data Factory output folder
            docker cp ${postgres_name}:/tmp/harmonized_clinical_data.csv ./output/harmonized_clinical_data.csv
            echo "Export csv created!"
        else
            echo "Error! The export csv was not created!"
            exit 1
        fi
    fi
fi
