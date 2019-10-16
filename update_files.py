#!/usr/bin/env python2
"""This script updates the jinja template files placed 
in template folder with the values of the config.json file.
"""

import logging
import os
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
                                             'docker-compose.yml'))
    vars = {'container_name': d_config['container_name'],
            'postgres_user': d_config['postgres_user'],
            'postgres_pwd': d_config['postgres_pwd'],
            'postgres_port': d_config['postgres_port'],
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
    vars = {'visit_file': imgconfig['input_files'][1],
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
                                             'build_dbs.sh'))
    vars = {'mipmap_db': d_config['mipmap_db'],
            'capture_db': d_config['capture_db'],
            'harmonize_db': d_config['harmonize_db'],
            'container_name': d_config['container_name'],
            'db_user': d_config['postgres_user'],
            'db_pwd': d_config['postgres_pwd'],
            'db_port': d_config['postgres_port']
            }
    template.stream(vars).dump('build_dbs.sh')



# Get the DataFactory configuration
with open('config.json') as json_data_file:
    config = json.load(json_data_file)

update_compose(config)
update_image_mapping(config)
update_bash_scripts(config)
