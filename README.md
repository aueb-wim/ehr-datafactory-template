# datafactory-wrapper

[![AUEB](https://img.shields.io/badge/AUEB-RC-red.svg)](http://rc.aueb.gr/el/static/home) [![HBP-SP8](https://img.shields.io/badge/HBP-SP8-magenta.svg)](https://www.humanbrainproject.eu/en/follow-hbp/news/category/sp8-medical-informatics-platform/)

This repo cocontains a wrapper script for running DataFactory EHR and MRI pipelines. 


## Prerequisites

* Python 2.x (see: https://www.python.org/)
* Python 3.x
* docker
* docker-compose
* Matlab (see: https://ch.mathworks.com/products/matlab.html)
* SPM12 deployed in /opt folder (see: https://www.fil.ion.ucl.ac.uk/spm/software/spm12/)
* Matlab engine for Python must be installed (see: https://www.mathworks.com/help/matlab/matlab_external/install-the-matlab-engine-for-python.html)
* The input folder must contain nifti files organized according to the following directory tree: subject/visit/protocol/repetition/ (see: https://github.com/HBPMedical/hierarchizer)
* If the protocol as per previous bullet-point is not T1, you'll have to update the protocol definition file from the mri-preprocessing-pipeline subproject (see: https://github.com/HBPMedical/mri-preprocessing-pipeline)

## Deployment and Configuration

There is an ansible script here for easy installation.

Otherwise, clone this repo on the hospital server and then:

Update config.json file. 

Run

```shell
python3 update_files.py
```
Create DataFactory databases

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

Copy and replace the mapping task configuration files from the mapping task design folder into the *preprocess_step*, *capture_step*, *harmonize_step* folders accordingly.

### Creating empty DataFactory databases


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

## Running DataFactory pipeline

### MRI pipeline

#### Running the Ashburner's NeuroMetric scripts

In DataFactory folder run 

```shell
python3 df.py mri -s <input folder> 
```

The input folder must contains nifti files according to the Prerequisites. 

#### Importing the volumetric brain features into the i2b2 capture database

In the folder where the output file `volumes.csv` is stored from the previous step, we must place a csv file with the name `mri_visits.csv`. This file must have the headers `PATIENT_ID`, `VISIT_ID`, `VISIT_DATE` and filled with these MRI information. The column `VISIT_DATE` must have the `dd/mm/yyyy` date format.

After this, in DataFactory folder run

```shell
python3 df.py imaging -s <input folder>
```

As `input folder` we give just the folder name and not the whole path.

### EHR pipeline

#### Preprocess step

In DataFactory folder run 

```shell
python3 df.py preprocess -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the folder name and not the whole path. 

#### Capture step

In DataFactory folder run

```shell
python3 df.py capture -s <input folder> -c <mapping config folder>
```

As `input folder` and `mapping config folder` we give just the folder name and not the whole path. 

### Harmonization step

In DataFactory folder run

```shell
python3 df.py harmonize -0 <output folder>
```

As `mapping config folder` we give just the folder name and not the whole path.

### Export flat csv

In DataFactory folder run

```shell
python3 df.py export -o <mapping config folder>
```

As `output folder` we give just the folder name and not the whole path.
`harmonized_clinical_data.csv` is created in this output folder 

### Anonymization

In DataFactory folder run

```shell
python3 df.py anonymize -o <anon output folder> -m <mode>
```

`-m` can take two flags `csv` or `db`. Choose `csv` for anonymizing a previously exported flat csv. Choose `db` for anonymizing the harmonized db and then exporting a flat csv. 

As `anon output folder` we give just the folder name and not the whole path.
`harmonized_clinical_anon_data.csv` is created in this output folder.