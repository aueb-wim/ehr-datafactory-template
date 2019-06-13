# ehr-datafactory-template

[![AUEB](https://img.shields.io/badge/AUEB-RC-red.svg)](http://rc.aueb.gr/el/static/home) [![HBP-SP8](https://img.shields.io/badge/HBP-SP8-magenta.svg)](https://www.humanbrainproject.eu/en/follow-hbp/news/category/sp8-medical-informatics-platform/)

This is a EHR DataFactory template with the folders and scripts needed to run the EHR DataFactory pipeline on a hospital server.

## Instructions

Update the `docker-compose.yml` with the valid credentials and ports of the postgres container which has already been running on the hospital server.
If the i2b2 database hasn't been created yet, edit the `build_dbs.sh` and update the `container_name` with the postgres container that is running on the server.
Also, update the postgres docker container details in `ingestdata.sh`

### DataFactory databases build

**Caution!** The following creation script drop any prexisting DataFactory database.

for creating all databases at once
```shell
$ sh build_dbs all
```

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

