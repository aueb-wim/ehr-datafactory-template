# ehr-datafactory-template

This is a EHR DataFactory template with the folders and scripts needed to run the EHR DataFactory pipeline on a hospital server.

## Instructions

Update the `docker-compose.yml` with the valid credentials and ports of the postgres container which has already been running on the hospital server.
If the i2b2 database hasn't been created yet, edit the `build_dbs.sh` and update the `container_name` with the postgres container that is running on the server.
Also, update the postgres docker container details in `ingestdata.sh`

for creating the database used by MIPMapReduce
```shell
$ sh build_dbs.sh mipmap
```

for creating the i2b2 capture database
```shell
$ sh build_dbs capture
```

for creating the i2b2 harmonize database
```shell
$ sh build_dbs harmonize
```
