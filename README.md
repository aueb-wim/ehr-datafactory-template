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
Alternatively, you can manually clone this repo in `/opt/DataFactory`. 

```shell
sudo git clone --recurse-submodules <repo_url> /opt/DataFactory
```

### DataFacotry user settings

Create user group `datafactory`:

```shell
sudo groupadd datafactory
```

Datafactory User must be in the user group `docker`, so the scripts will run without the “sudo” command. To do that, follow the below instructions:

Add the `docker` group if it doesn't already exist:

```shell
 sudo groupadd docker
 ```

Add the connected user "$USER" to the `docker` and `datafactory` group. Change the user name to match your preferred user if you do not want to use your current user:

```shell
sudo gpasswd -a $USER docker
sudo gpasswd -a $USER datafactory
```

**log out/in to activate the changes to groups.**

You can use `$ docker run hello-world`  to check if you can run docker without sudo.

### Install python packages

In /opt/DataFactory folder run:

```shell
pip3 install -r requirements.txt 
```

In case of `locale.Error: unsupported locale setting` give the below command and rerun the above pip command:

```shell
export LC_ALL=C
```

### Data Factory reserved ports

* 55432: PostgreSQL container
* 8082: MySQL 5.7 container for LORIS-for-MIP (DF optional feature)
* 8088: apache-php container hosting LORIS (DF optional feature)

Those ports can be changed in the configuration. The PostgreSQL container port can be changed in the `config.json`. The LORIS-for-MIP ports can be changed in the docker-compose.yml file (see LORIS-for-MIP readme file)
### Data Factory Folders

The `config.json` contains the following folder structure as default.

| Path                                             | Description                                   |
| ------------------------------------------------ | --------------------------------------------- |
| /data/DataFactory                                | DataFactory data folder                       |
| /data/DataFactory/EHR                            | EHR data root input folder                    |
| /data/DataFactory/MRI/dicom/raw                  | DICOM raw data root input folder              |
| /data/DataFactory/MRI/nifti/organized            | DICOM root input folder with organized files  |
| /data/DataFactory/MRI/nifti/raw                  | NIFTI raw data root input folder              |
| /data/DataFactory/MRI/nifti/organized            | NIFTI root input folder with organized files  |
| /data/DataFactory/imaging                        | imaging data root input folder                |
| /data/DataFactory/output/                        | output root folder                            |
| /data/DataFactory/anonymized_output/             | anonymized output root folder                 |
| /opt/DataFactory/dbproperties                    | db properties folder                          |
| /opt/DataFactory/mipmap_mappings/preprocess_step | preprocess step config root folder            |
| /opt/DataFactory/mipmap_mappings/capture_step    | capture step config root folder               |
| /opt/DataFactory/mipmap_mappings/harmonize_step  | harmonize step config root folder             |
| /opt/DataFactory/mipmap_mappings/imaging_step    | imaging mapping config folder                 |
| /opt/DataFactory/export_step                     | export sql scripts folder                     |


Change the group ownership of DataFactory installation folder `/opt/DataFactory` to the `datafactory` group and give write and execute rights:

```shell
sudo chgrp -R datafactory /opt/DataFactory
sudo chmod -R g+xw /opt/DataFactory
```

Update the parameteres in config.json file (Hospital name, docker postgres container details etc).
Then run:

```shell
$ sudo ./update_files.py
```

Then change the group ownership of the DataFactory data folder to the `datafactory` group and give writing permitions:
```shell
sudo chgrp -R datafactory <datafactory data folder>
sudo chmod -R g+w <datafactory data folder>
```

The default datafactory data folder is `/data/DataFactory`

### LORIS-for-MIP setup (optional)

If we want to add MRI Quality Control functionality to the MIP's DataFactory (as an extra step prior to the MRI volumetric brain feature extraction pipeline), we have the option of installing [LORIS-for-MIP](https://github.com/HBPMedical/LORIS-for-MIP). Please refer to the repo's README for further information and installation instructions. 

After installing the LORIS-for-MIP, we must create a soft link between DataFactory's folder that contains the batches of organized NIFTI files, and the output folder of LORIS-for-MIP. Do do so, we give in the command line:

```shell
ln -s /data/DataFactory/MRI/nifti/organized /data/LORIS/nifti_out
```


### Create the DataFactory databases

**Caution!** The following creation script drops any prexisting DataFactory database. Skip this step if there is no such need *i.e. when importing a second batch of hospital data.* 

for dropping and creating all databases at once

```shell
$ sh build_dbs.sh all
```

Ignore any warning messages about variables `mipmap_map`, `mipmap_source` and `mipmap_pgproperties`. 

You can use the psql terminal tool or pgAdmin to recheck if the three DataFactory databases have been created (`mipmap`, `i2b2_capture`, `i2b2_harmonized`).

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

## Mapping configuration files creation and upload
For creating the mapping configuration files, we use a [mapping-desinging-template](https://github.com/HBPMedical/ehr-mapping-design-template.git) in a local machine (not in the hospital node) with a graphical desktop environment. Please refer to the [DataFactory's User Guide](https://mip.ebrains.eu/documentation/User%20Manuals/6) for further instructions.

When, the mapping configuration files are ready, we upload them to the hospital node and placed them in the designated subfolders inside the folder `/opt/DataFactory/mipmap_mappings`.

For the preprocess step create a new folder in `/preprocess_step` and name it accordingly (ie `config1` if is the first version of this configuration) and then place the configuration files in. 

For the capture step create a new folder in `/capture_step` and name it accordingly (ie `config1` if is the first version of this configuration) and then place the configuration files in.

For the harmonize step create a new folder in `/harmonize_step` and name it accordingly (ie `config1` if is the first version of this configuration) and then place the configuration files in.

**Please change the user group and the rights of the folders in order to be readable, writable and executable for the `datafactory` user group.**

```shell
sudo chgrp -R datafactory  <configuration foler>
sudo chmod g+wrx -R <configuration foler>
```

**Caution** When using **Filezilla** for ftp uploading the configuration files, change the tranfer setting to binary! Otherwise the line endings are changed and scripts will be broken!  

## DataFactory data folders

The ehr files must be placed in a subfolder in the folder `/data/DataFactory/EHR` and named accordingly  (ie `batch1` if is the first batch of data)

The NIFTI files must be placed in a subfolder in the folder `/data/DataFactory/MRI/nifti/raw` and named accordingly (For example, in case we have a first batch of MRIs, we place them into the folder `/data/DataFactory/MRI/dicom/raw/batch1`.). The files must contain full-brain T1-weighted MRI data in nifti format and named `<patient_id>_<visit_id>.nii`.

When [LORIS-for-MIP](https://github.com/HBPMedical/LORIS-for-MIP) is installed, we could use DICOM files instead of NIFTI as raw MRI input. Please refer to LORIS-for-MIP github repo for further information. 

## Running DataFactory pipeline

### MRI pipeline

#### Running the Ashburner's NeuroMetric scripts

In DataFactory folder run

```shell
./df.py mri [--loris] --input_folder <input folder>
```

`--input folder` we give the subfolder name in `/data/DataFactory/MRI/nifti/raw/` and not the whole path. For example just `batch1`, `batch2` etc. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/data/DataFactory/MRI/nifti/raw/`.

`--loris` is a boolean flag and when is present, we give as `input folder` the subfolder name in `/data/DataFactory/MRI/nifti/organized` (just the name and not the whole path).

**Note** It is assumed that the NIFTI files are already organized by the [LORIS-for-MIP](https://github.com/HBPMedical/LORIS-for-MIP) module.  

The output of this step is a `volumes.csv` file with the volumetric data of all the MRIs. This file is stored in a subfolder with the same name given as `input_folder` in the folder `/data/DataFactory/imaging`. For example `/data/DataFactory/imaging/batch1`.  

#### Importing the volumetric brain features into the i2b2 capture database

In the folder where the output file `volumes.csv` is stored from the previous step, we place a csv file with the name `mri_visits.csv`. This file must have the headers `PATIENT_ID`, `VISIT_ID`, `VISIT_DATE` and be filled with this MRI information. The column `VISIT_DATE` must have the `dd/mm/yyyy` date format.

Then, in DataFactory folder run

```shell
./df.py ingest imaging  --input_folder <input folder>
```

`--input folder` we give just the subfolder name in `/data/DataFactory/imaging` and not the whole path. For example just `batch1`, `v1` or `batch2` etc. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/data/DataFactory/imaging`.

### EHR pipeline

#### Preprocess step

In DataFactory folder run

```shell
./df.py ingest ehr preprocess --input_folder <input folder> --config_folder <mapping config folder>
```

`--input_folder` we give just the corresponding subfolder name (i.e `batch1`, `v1` or `batch2`), where the ehr data files are stored, and not the whole path. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/data/DataFactory/EHR/input/`.
`--config_folder` we give just the corresponding subfolder name (i.e. `config1`, `config2` etc), where the preprocessing configuration files are stored, and not the whole path. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/opt/DataFactory/mipmap_mappings/preprocess_step/`.

Auxiliary files are created in the same folder where the ehr csv files are located (for example in `/data/DataFactory/EHR/input/batch1` if we have used as input folder the name `batch1`)

#### Capture step

In DataFactory folder run

```shell
./df.py ingest ehr capture --input_folder <input folder> --config_folder <mapping config folder>
```

`--input_folder` we give just the corresponding subfolder name, where the ehr data files are stored, and not the whole path. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/data/DataFactory/EHR/input/`.
`--config_folder` we give just the corresponding subfolder name (i.e. `config1`, `config2` etc), where the mapping configuration files are stored, and not the whole path. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/opt/DataFactory/mipmap_mappings/capture_step/`.


#### Harmonization step

In DataFactory folder run

```shell
./df.py ingest ehr harmonize --config_folder <mapping config folder>
```

`--config_folder` we give just the corresponding subfolder name (i.e. `config1`, `config2` etc), where the mapping configuration files are stored, and not the whole path. When this flag is skipped the user will be prompted to select one of the existing subfolders in `/opt/DataFactory/mipmap_mappings/harmonize_step/`.

### Export flat csv

In DataFactory folder run

```shell
./df.py export  -s <flattening method> -d <string value> [--csv_name <flat csv name>] [--local] <output folder>
```

`-s, --strategy` we declare the csv flattening method. The choices (defined in config.json) are the following(if left blank, the user will be prompted to select the following options):

           1. 'mindate': For each patient, export one row with all information related to her first visit
           
           2. 'maxdate': For each patient, export one row with all information related to her last visit
           
           3. '6months': For each patient, export one row with the information according to the 6-month window selection strategy defined by CHUV for clinical data related to MRI's. The criteria in detail:
               - For a patient to have a row in the output CSV she has to have an MRI and a VALID Diagnosis (etiology1 !=“diagnostic en attente” and etiology1 != “NA”) in a 6-month window to the MRI. If there are more than one MRIs choose the first one. If there are more than one Diagnosis, choose the closest VALID to the MRI.
               - The age and the visit date selected are the ones of the Diagnosis.
               - Having information about MMSE and MoCA is optional. Has to be within a 6-month window to the Diagnosis date.
               
           4. 'longitude': For each patient, export all available information.

`-d, --dataset` we declare the value that is going to be filled in the final csv under the column 'Dataset'.
`--csv_name` (optional) if it is given, it overrides the defalut strategy's flat csv name (this is declared in config.json) with the given value.
`--local` is a boolean option and when is present, the flat csv is exported from the `i2b2 capture` database with the hospital's local variables only without any CDEs.
As `output folder` we give just the folder name and not the whole path where the flat csv file is created.

### Anonymization

For Anonymization we have 2 options:

1. (db) Copy the i2b2 harmonized db and anonymize it, and then export the flat csv with a given strategy.

2. (csv) Anonymize a previously exported flattened csv.

Also, there are currently 2 anonymization hash methods: **md5** and **sha224**. We can declare which of those 2 methods is going to be used in the DataFactory anonymization step by updating the `hash_method` field in `config.json` file.

For the case `1 (db)` , in DataFactory folder run:

```shell
./df.py anonymize db -s <flattening method>  --hash_function <hash_method> -d <string value> [--csv_name <csv file name>] <output_folder>  
```

`-s, --strategy` we declare the csv flattening method (look export step). If left blank, the user will be prompted to select a strategy.

`--hash_function` we declare the hash funtion that is goint to be used for anonymization [md5 | sha224]. If the user, the user will be prompted to select hash_function.

`-d, --dataset` we declare the value that is going to be filled in the final csv under the column 'Dataset'.

`--csv_name` (optional) if it is given, it overrides the defalut strategy's flat csv name (this is declared in config.json) with the given value.

`output_folder` we give just the folder name (not the whole path) where the output anonymized csv will be placed. This folder is located in `/data/DataFactory/anonymized_output/`

**Important Note**
*Make sure that there is no active connection to any DataFactory Databases*

For the  case `2 (csv)`, in DataFactory folder run:

```shell
./df.py anonymize csv  --hash_function <hash method> -d <string value> --csv_name <anonymized csv file name> <source csv path> <output_folder>
```

--`hash_function` we declare the hash funtion that is going to be used for anonymization [md5 | sha224]. If the user, the user will be prompted to select hash_function.

`-d, --dataset` we declare the value that is going to be filled in the final csv under the column 'Dataset'.

`--csv_name` we give the anonymized csv file name

`source csv path` the filepath of the input csv that is going to be anonymized

`output_folder` we give just the folder name (not the whole path) where the output anonymized csv will be placed. This folder is located in `/data/DataFactory/anonymized_output/`


### Interactive mode 

The user can execute the ehr pipeline steps (preprocess, capture, harmonize, export) interectively through user prompts in terminal. Currently, the interactive mode does not support the mri step. 

```shell
./df.py interactive
```
