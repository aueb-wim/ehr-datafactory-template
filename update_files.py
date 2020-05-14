#!/usr/bin/env python3
"""This script updates the jinja template files placed 
in template folder with the values of the config.json file.
"""

import logging
import os
import grp
import json
from jinja2 import Environment, FileSystemLoader
from logger import LOGGER


def update_compose(configdict):
    LOGGER.info('Updating docker-compose.yml')
    d_config = configdict['db_docker']
    my_path = os.path.abspath(os.path.dirname(__file__))
    env_path = os.path.join(my_path, 'templates')
    env = Environment(loader=FileSystemLoader(env_path))
    template = env.get_template(os.path.join('docker-compose',
                                             'docker-compose.yml.j2'))
    vars = {'postgresql_container': d_config['container_name'],
            'postgresql_user': d_config['postgres_user'],
            'postgresql_pwd': d_config['postgres_pwd'],
            'postgresql_port': d_config['postgres_port'],
            'capture_db': d_config['capture_db'],
            'harmonize_db': d_config['harmonize_db'],
            'mipmap_db': d_config['mipmap_db']
            }
    template.stream(vars).dump('docker-compose.yml')


def update_image_mapping(configdict):
    LOGGER.info('Updating imaging patient and encounter properties files')
    imgconfig = configdict['mipmap']['imaging']['mapping']
    my_path = os.path.abspath(os.path.dirname(__file__))
    env_path = os.path.join(my_path, 'templates')
    env = Environment(loader=FileSystemLoader(env_path))
    img_mapping_folder = configdict['mipmap']['imaging']['root']
    template1 = env.get_template(os.path.join('image_mapping',
                                              'patientmapping.properties.j2'))
    template2 = env.get_template(os.path.join('image_mapping',
                                              'encountermapping.properties.j2'))
    LOGGER.info('Hospital name is %s' % configdict['hospital_name'])
    LOGGER.info('MRI visit file name is %s' % imgconfig['input_files'][1])
    hospital_name = '\"' + configdict['hospital_name'] + '\"'
    vars = {'mri_visit_file': imgconfig['input_files'][1],
            'hospital_name': hospital_name}
    patientprop_fp = os.path.join(img_mapping_folder,
                                 'patientmapping.properties')
    encounterprop_fp = os.path.join(img_mapping_folder,
                                    'encountermapping.properties')
    template1.stream(vars).dump(patientprop_fp)
    template2.stream(vars).dump(encounterprop_fp)


def update_bash_scripts(config):
    LOGGER.info('Updating bash script for building df dbs')
    d_config = config['db_docker']
    my_path = os.path.abspath(os.path.dirname(__file__))
    env_path = os.path.join(my_path, 'templates')
    env = Environment(loader=FileSystemLoader(env_path))
    template = env.get_template(os.path.join('bash_scripts',
                                             'build_dbs.sh.j2'))
    vars = {'mipmap_db': d_config['mipmap_db'],
            'capture_db': d_config['capture_db'],
            'harmonize_db': d_config['harmonize_db'],
            'postgresql_container': d_config['container_name'],
            'postgresql_user': d_config['postgres_user'],
            'postgresql_pwd': d_config['postgres_pwd'],
            'postgresql_port': d_config['postgres_port']
            }
    template.stream(vars).dump('build_dbs.sh')


def create_df_data_folders(config):
    
    df_output_path = config['flattening']['output_folder']
    os.makedirs(df_output_path, exist_ok=True)
    
    df_anon_output_path = config['anonymization']['output_folder']
    os.makedirs(df_anon_output_path, exist_ok=True)

    df_ehr_input_path = config['mipmap']['input_folders']['ehr']
    os.makedirs(df_ehr_input_path, exist_ok=True)

    df_imaging_input_path = config['mipmap']['input_folders']['imaging']
    os.makedirs(df_imaging_input_path, exist_ok=True)

    df_nmm_nifti_raw_input = config['mri']['input_folders']['nifti']['raw']
    os.makedirs(df_nmm_nifti_raw_input, exist_ok=True)

    df_nmm_nifti_input = config['mri']['input_folders']['nifti']['organized']
    os.makedirs(df_nmm_nifti_input, exist_ok=True)

    df_nmm_dicom_raw_input = config['mri']['input_folders']['dicom']['raw']
    os.makedirs(df_nmm_dicom_raw_input, exist_ok=True)

    df_nmm_dicom_input = config['mri']['input_folders']['dicom']['organized']
    os.makedirs(df_nmm_dicom_input, exist_ok=True)

    df_nmm_spm12_output = config['mri']['output_folders']['spm12']
    os.makedirs(df_nmm_spm12_output, exist_ok=True)

    # Get the data input output datafactory root folder
    #df_data_path = os.path.commonpath([df_ehr_input_path, df_imaging_input_path])

    # get the GID of datafactory group
    #df_gid = grp.getgrnam('datafactory').gr_gid

    # Change the group ownership from root to datafactory
    #os.chown(df_data_path, uid=-1, gid=df_gid,)

# Get the DataFactory configuration
with open('config.json') as json_data_file:
    config = json.load(json_data_file)

update_compose(config)
update_image_mapping(config)
update_bash_scripts(config)
create_df_data_folders(config)
