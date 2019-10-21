# datafactory-wrapper

[![AUEB](https://img.shields.io/badge/AUEB-RC-red.svg)](http://rc.aueb.gr/el/static/home) [![HBP-SP8](https://img.shields.io/badge/HBP-SP8-magenta.svg)](https://www.humanbrainproject.eu/en/follow-hbp/news/category/sp8-medical-informatics-platform/)

This repo contains a wrapper script for running DataFactory EHR and MRI pipelines. 


## Prerequisites

* Python 2.x (see: https://www.python.org/)
* docker
* docker-compose
* Matlab (see: https://ch.mathworks.com/products/matlab.html) (**MRI pipeline**)
* pandas 1.24.2 for python version 2 (**MRI pipeline**)
* SPM12 deployed in /opt folder (see: https://www.fil.ion.ucl.ac.uk/spm/software/spm12/)(**MRI pipeline**)
* Matlab engine for Python must be installed (see: https://www.mathworks.com/help/matlab/matlab_external/install-the-matlab-engine-for-python.html) (**MRI pipeline**)
* The input folder must contain nifti files organized according to the following directory tree: subject/visit/protocol/repetition/ (see: https://github.com/HBPMedical/hierarchizer)(**MRI pipeline**)
* If the protocol as per previous bullet-point is not T1, you'll have to update the protocol definition file from the mri-preprocessing-pipeline subproject (see: https://github.com/HBPMedical/mri-preprocessing-pipeline) (**MRI pipeline**)

## Deployment and Configuration

There is an ansible script [here](https://github.com/aueb-wim/ansible-datafactory) for installing and creating the datafactory data folders.


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

```shell
 ./update_files.py
```

Then create DataFactory databases

**Caution!** The following creation script drop any prexisting DataFactory database. Skip this step if there is no such need *i.e. when importing a second batch of hospital data.* 

for droping and creating all databases at once

```shell
$ sh build_dbs.sh all
```

Ignore any warning messages about variables `mipmap_map`, `mipmap_source` and `mipmap_pgproperties`. If everything gone well you will see the following output:

```shell
i2b2-setup-harmonized exited with code 0
i2b2-setup exited with code 0
```

You can use the psql terminal tool or pgAdmin to recheck if the three DataFactory databases are created.

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

For the proprocess step we create a new folder in `/preprocess_step` and named it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

For the capture step we create a new folder in `/capture_step` and named it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

For the harmonize step we create a new folder in `/harmonize_step` and named it accordingly (ie `1` if is the first version of this configuration) and then place the configuration files in.

**Please change the rights of the folders in order to be readable and writable for the `datafactory` user group.**

## DataFactory data folders

The ehr files must be placed in a subfolder in the folder `/data/DataFactory/EHR/input` and named it accordingly  (ie `1` if is the first batch of data)
Please check the documentation for more information about the ehr files in the repository where the hospital's mapping tasks configuration files are stored. 

## Running DataFactory pipeline

### MRI pipeline

#### Running the Ashburner's NeuroMetric scripts

In DataFactory folder run

```shell
./df.py mri -s <input folder>
```

The input folder must contains nifti files according to the Prerequisites. 

#### Importing the volumetric brain features into the i2b2 capture database

In the folder where the output file `volumes.csv` is stored from the previous step, we must place a csv file with the name `mri_visits.csv`. This file must have the headers `PATIENT_ID`, `VISIT_ID`, `VISIT_DATE` and filled with these MRI information. The column `VISIT_DATE` must have the `dd/mm/yyyy` date format.

After this, in DataFactory folder run

```shell
./df.py imaging -s <input folder>
```

As `input folder` we give just the subfolder name in `/data/DataFactory/EHR/input` and not the whole path.

### EHR pipeline

#### Preprocess step

In DataFactory folder run

```shell
./df.py preprocess -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the corresponding subfolder name and not the whole path.

#### Capture step

In DataFactory folder run

```shell
./df.py capture -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the corresponding subfolder name and not the whole path.

### Harmonization step

In DataFactory folder run

```shell
./df.py harmonize -c <mapping config folder>
```

As `mapping config folder` we give jjust the corresponding subfolder name and not the whole path.

### Export flat csv

In DataFactory folder run

```shell
./df.py export -o <output folder>
```

As `output folder` we give just the folder name and not the whole path.
`harmonized_clinical_data.csv` is created in this output folder 

### Anonymization

In DataFactory folder run

```shell
./df.py anonymize -o <anon output folder> -m <mode>
```

`-m` can take two flags `csv` or `db`. Choose `csv` for anonymizing a previously exported flat csv. Choose `db` for anonymizing the harmonized db and then exporting a flat csv.

As `anon output folder` we give just the folder name and not the whole path.
`harmonized_clinical_anon_data.csv` is created in this output folder.
