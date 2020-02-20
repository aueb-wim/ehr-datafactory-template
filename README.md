# datafactory-wrapper

[![AUEB](https://img.shields.io/badge/AUEB-RC-red.svg)](http://rc.aueb.gr/el/static/home) [![HBP-SP8](https://img.shields.io/badge/HBP-SP8-magenta.svg)](https://www.humanbrainproject.eu/en/follow-hbp/news/category/sp8-medical-informatics-platform/)

This repo contains a wrapper script for running DataFactory EHR and MRI pipelines.

## Prerequisites

* Python 2.x (see: https://www.python.org/)
* docker
* docker-compose
* Matlab (see: [here](https://ch.mathworks.com/products/matlab.html)) (**MRI pipeline**)
* pandas 1.24.2 for python version 2 (**MRI pipeline**)
* SPM12 deployed in /opt folder (see: [here](https://www.fil.ion.ucl.ac.uk/spm/software/spm12/))(**MRI pipeline**)
* Matlab engine for Python must be installed (see: [here](https://www.mathworks.com/help/matlab/matlab_external/install-the-matlab-engine-for-python.html)) (**MRI pipeline**)

## Deployment and Configuration

It is suggested to use the ansible script [here](https://github.com/aueb-wim/ansible-datafactory) for installing and creating the datafactory data folders.
Alternatively, you can manually clone this repo, update the `config.json` and create the following folder structure.

### Data Factory Folders

| Path                                             | Description                                   |
| ------------------------------------------------ | --------------------------------------------- |
| /opt/DataFactory                                 | DataFactory main folder                       |
| /opt/DataFactory/dbproperties                    | DataFactory db properties folder              |
| /data/DataFactory/EHR/input                      | DataFactory EHR data root input folder        |
| /data/DataFactory/MRI/dicom/raw                  | DataFactory DICOM raw data root input folder  |
| /data/DataFactory/MRI/nifti/raw                  | DataFactory NIFTI raw data root input folder  |
| /data/DataFactory/imaging                        | DataFactory imaging data root input folder    |
| /data/DataFactory/output/                        | DataFactory output root folder                |
| /data/DataFactory/anonymized_output/             | DataFactory anonymized output root folder     |
| /opt/DataFactory/mipmap_mappings/preprocess_step | DataFactory preprocess step config root folder|
| /opt/DataFactory/mipmap_mappings/capture_step    | DataFactory capture step config root folder   |
| /opt/DataFactory/mipmap_mappings/harmonize_step  | DataFactory harmonize step config root folder |
| /opt/DataFactory/mipmap_mappings/imaging_step    | DataFactory imaging mapping config folder     |
| /opt/DataFactory/export_step                     | DataFactory export sql scripts folder         |

### Data Factory enviroment variables

| Variable name            | Description                                    |
| ------------------------ | ---------------------------------------------- |
| DF_PATH                  | path of DataFactory scripts folder             |
| DF_DATA_PATH             | path of DataFactory DATA folder                |

### Install python packages

In /opt/DataFactory folder run:

```shell
pip install -r requirements.txt --user
```

### update files

Update the parameteres in config.json file (Hospital name, docker postgres container details etc).
Then run:

```shell
 ./update_files.py
```

Then create the DataFactory databases

**Caution!** The following creation script drops any prexisting DataFactory database. Skip this step if there is no such need *i.e. when importing a second batch of hospital data.* 

for dropping and creating all databases at once

```shell
$ sh build_dbs.sh all
```

Ignore any warning messages about variables `mipmap_map`, `mipmap_source` and `mipmap_pgproperties`. If everything gone well you will see the following output:

```shell
i2b2-setup-harmonized exited with code 0
i2b2-setup exited with code 0
```

You can use the psql terminal tool or pgAdmin to recheck if the three DataFactory databases have been created.

If you want to create a specific database give the following command:

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

## Mapping upload

The mapping configuration files are placed in subfolders in the folder `/opt/DataFactory/mipmap_mappings`.

For the preprocess step create a new folder in `/preprocess_step` and name it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

For the capture step create a new folder in `/capture_step` and name it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

For the harmonize step create a new folder in `/harmonize_step` and name it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

**Please change the rights of the folders in order to be readable and writable for the `datafactory` user group.**

## DataFactory data folders

The ehr files must be placed in a subfolder in the folder `/data/DataFactory/EHR/input` and named accordingly  (ie `1` if is the first batch of data)
Please check the documentation for more information about the ehr files in the repository where the hospital's mapping tasks configuration files are stored.

The niftii files must be placed in a subfolder in the folder `/data/DataFactory/MRI/nifti/raw` and named accordingly (For example, in case we have a first batch of MRIs, we place them into the folder `/data/DataFactory/MRI/dicom/raw/1`.). The files must contain full-brain T1-weighted MRI data in nifti format and named `<patient_id>_<visit_id>.nii`.

## Running DataFactory pipeline

### MRI pipeline

#### Running the Ashburner's NeuroMetric scripts

In DataFactory folder run

```shell
./df.py mri -s <input folder>
```

As `input folder` give the subfolder name in `/data/DataFactory/MRI/dicom/raw/` and not the whole path. For example just `1`, `v1` or `2` etc.
The output of this step is a `volumes.csv` file with the volumetric data of all the MRIs. This file is stored in a subfolder with the same name given as `input_folder` in the folder `/data/DataFactory/imaging`. For example `/data/DataFactory/imaging/1`.  


#### Importing the volumetric brain features into the i2b2 capture database

In the folder where the output file `volumes.csv` is stored from the previous step, we place a csv file with the name `mri_visits.csv`. This file must have the headers `PATIENT_ID`, `VISIT_ID`, `VISIT_DATE` and be filled with this MRI information. The column `VISIT_DATE` must have the `dd/mm/yyyy` date format.

Then, in DataFactory folder run

```shell
./df.py imaging -s <input folder>
```

As `input folder` give just the subfolder name in `/data/DataFactory/imaging` and not the whole path. For example just `1`, `v1` or `2` etc.

### EHR pipeline

#### Preprocess step

In DataFactory folder run

```shell
./df.py preprocess -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the corresponding subfolder name and not the whole path. `Input folder` is where the ehr data files are stored and `mapping config folder` is where the mapping configuration files are stored. Auxiliary files are created in the same folder where the ehr csv files are located (for example in `/data/DataFactory/EHR/input/1` if we have used as input folder the name `1`)

#### Capture step

In DataFactory folder run

```shell
./df.py capture -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the corresponding subfolder name and not the whole path. `Input folder` is where the data files are stored and `mapping config folder` is where the mapping configuration files are stored.

### Harmonization step

In DataFactory folder run

```shell
./df.py harmonize -c <mapping config folder>
```

As `mapping config folder` we give just the corresponding subfolder name and not the whole path. (i.e. `1`, `2` etc).`mapping config folder` is where the mapping configuration files are stored.

### Export flat csv

In DataFactory folder run

```shell
./df.py export -o <output folder> (--csv_name <flat csv name>) --strategy <flattening method> --dataset <string value>
```

As `output folder` we give just the folder name and not the whole path where the flat csv file is created.
`--csv_name` (optional) if it is given, it overrides the defalut strategy's flat csv name with the given value.
`--strategy` we declare the csv flattening method. The choices (defined in config.json) are the following:
           1. 'mindate': For each patient, export one row with all information related to her first visit
           2. 'maxdate': For each patient, export one row with all information related to her last visit
           3. '6months': For each patient, export one row with the information according to the 6-month window selection strategy defined by CHUV for clinical data related to MRI's. The criteria in detail:
               - For a patient to have a row in the output CSV she has to have an MRI and a VALID Diagnosis (etiology1 !=“diagnostic en attente” and etiology1 != “NA”) in a 6-month window to the MRI. If there are more than one MRIs choose the first one. If there are more than one Diagnosis, choose the closest VALID to the MRI.
               - The age and the visit date selected are the ones of the Diagnosis.
               - Having information about MMSE and MoCA is optional. Has to be within a 6-month window to the Diagnosis date.
           4. 'longitude': For each patient, export all available information.
`--dataset` we declare the value that is going to be filled in the final csv under the column 'Dataset'.

### Anonymization

For Anonymization we have 2 options:

1. Copy the i2b2 harmonized db and anonymize it, and then export the flat csv with a given strategy.

2. Anonymize a previously exported flattened csv.


Also, there are currently 2 anonymization hash methods: **md5** and **sha224**. We can declare which of those 2 methods is going to be used in the DataFactory anonymization step by updating the `hash_method` field in `config.json` file.

For the case `1` , in DataFactory folder run:

```shell
./df.py anonymize -m db -o <anon output folder>  (--csv_name <csv file name>) --strategy <flattening method>  --dataset <string value>
```

`-o` we give just the folder name (not the whole path) where the output anonymized csv will be placed. This folder is located in `/data/DataFactory/anonymized_output/`

`--csv_name` (optional) if it is given, it overrides the defalut strategy's flat csv name with the given value.

`--strategy` we declare the csv flattening method.

`--dataset` we declare the value that is going to be filled in the final csv under the column 'Dataset'.

For the  case `2`, in DataFactory folder run:

```shell
./df.py anonymize -m csv -s <source csv folder> --csv_anon <input csv file name> -o <anon output folder> --csv_name <output csv file name>
```

As `anon output folder` in `-o` keyword,  we give just the folder name and not the whole path (for example `1`).

`-s` we give the folder name, located in `/data/DataFactory/output`, which contains the input flat csv.

`--csv_anon` we give the input flat csv which is going to be anonymized.

`-o` we give just the folder name (not the whole path) where the output anonymized csv will be placed. This folder is located in `/data/DataFactory/anonymized_output/`

`--csv_name` we give the final anonymized csv name.
