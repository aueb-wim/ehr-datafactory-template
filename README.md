# ehr-datafactory-template

[![AUEB](https://img.shields.io/badge/AUEB-RC-red.svg)](http://rc.aueb.gr/el/static/home) [![HBP-SP8](https://img.shields.io/badge/HBP-SP8-magenta.svg)](https://www.humanbrainproject.eu/en/follow-hbp/news/category/sp8-medical-informatics-platform/)

This is a EHR DataFactory template with the folders and scripts needed to run the EHR DataFactory pipeline on a hospital server.

## Deployment and Configuration

Clone this repo on the hospital server.

Update the `docker-compose.yml` with the valid credentials and ports of the postgres container which has already been running on the hospital server.

If the i2b2 database hasn't been created yet, edit the `build_dbs.sh` and update the `container_name` with the postgres container that is running on the server.

Also, update the postgres docker container details in `ingestdata.sh`

Copy and replace the mapping task configuration files from the mapping task design folder into the *preprocess_step*, *capture_step*, *harmonize_step* folders accordingly.

### Creating empty DataFactory databases

**Caution!** The following creation script drop any prexisting DataFactory database. Skip this step if there is no such need *i.e. when importing a second batch of hospital data.*


for droping and creating all databases at once
```shell
$ sh build_dbs.sh all
```

for creating an empty database used by MIPMapReduce
```shell
$ sh build_dbs.sh mipmap
```

for creating am empty i2b2 capture database
```shell
$ sh build_dbs.sh capture
```

for creating an empty i2b2 harmonize database
```shell
$ sh build_dbs.sh harmonize
```

## Running EHR DataFactory pipeline

### Step_1 - preprocess step

In DataFactory folder run

```shell
sh ingestdata.sh preprocess
```

Auxiliary files are created in the same folder where the hospital csv files are located.

### Step_2 - capture step

In DataFactory folder run 

```shell
sh ingestdata.sh capture
```


### Step_3 - harmonization step

In DataFactory folder run

```shell
sh ingestdata.sh harmonize
```

### Step_4 - local data flattening step

In DataFactory folder run 

```shell
sh ingestdata.sh export
```

harmonized_clinical_data.csv is created in the mipmap output folder 

### Step_5 - Anonymization step

In DataFactory folder run

```shell
sh anonymize.sh i2b2
```

### Step_6 - anonymized data flattening step

In DataFactory folder run

```shell
sh anonymize.sh export
```

harmonized_clinical_anon_data.csv is created in the mipmap anonym_output folder