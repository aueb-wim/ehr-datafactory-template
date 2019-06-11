#! /bin/bash


mipmap='mipmap'
db_capture='i2b2_capture'
db_harmonized='i2b2_harmonized' 
container_name=<update with postgres name container>

if [ $1 = "mipmap"]; then
# create mipmap database
    echo "Creating $mipmap database"
    docker exec -it $container_name psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS $mipmap;"
    docker exec -it $container_name psql -U postgres -d postgres -c "CREATE DATABASE $mipmap;"
fi

if [ $1 = "capture"]; then
# create and set up capture database
    echo "Creating $db_capture database"
    docker exec -it $container_name psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS $db_capture;"
    docker exec -it $container_name psql -U postgres -d postgres -c "CREATE DATABASE $db_capture;"
    echo "Setting up $db_capture database"
    docker-compose run --rm i2b2_setup
fi

if [ $1 = "harmonize"]; then
# create and set up harmonized database
    echo "Creating $db_harmonized database"
    docker exec -it $container_name psql -U postgres -d postgres -c "DROP DATABASE IF EXISTS $db_harmonized;"
    docker exec -it $container_name psql -U postgres -d postgres -c "CREATE DATABASE $db_harmonized;"
    echo "Setting up $db_harmonized database"
    docker-compose run --rm i2b2_setup_harmonized
fi