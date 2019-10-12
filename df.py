#!/usr/bin/env python3
import os
import sys
import argparse
import json
import tarfile
import docker
from helpers_docker import get_from, copy_to
from helpers_imaging import split_subjectcode
from logger import LOGGER
from anonymization-4-federation.anonymize_csv import anonymize_csv


def run_docker_compose(source_folder, cnfg_folder, dbprop_folder):
    os.environ['mipmap_pgproperties'] = dbprop_folder
    LOGGER.info('Mounting %s as source folder' % source_folder)
    os.environ['mipmap_source'] = source_folder
    LOGGER.info('Mounting %s as mapping folder' % config_folder)
    os.environ['mipmap_map'] = cnfg_folder
    LOGGER.info('Running... docker-compose up mipmap_etl')
    os.system('docker-compose up mipmap_etl')
    LOGGER.info('Removing mipmap container')
    os.system('docker rm mipmap')


def export_csv(output_folder, csv_name, sql_script, container, config):
    """Exports a flat csv from i2b2"""
    db_user = config['db_docker']['postgres_user']
    i2b2_name = config['db_docker']['harmonize_db']
    sql_folder = config['sql_scripts_folder']
    LOGGER.info('Performing EHR DataFactory export step')
    # try to delete any existing flattened csv in postgres container
    try:
        cmd_rm_csv = 'rm -rf /tmp/%s' % csv_name
        container.exec_run(cmd_rm_csv)
    except:
        pass
    # Copy the sql script to the postgres container
    sql_script_path = os.path.join(sql_folder, sql_script)
    copy_to(sql_script_path, '/tmp/', container)
    # run the sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user,
                                                  i2b2_name,
                                                  sql_script)
    LOGGER.info('Excecuting pivoting sql script...')
    container.exec_run(cmd_sql)
    # check if the output folder exist, create it otherwise
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
        except OSError:
            LOGGER.warning('Creation of the output directory %s failed' % output_folder)
        else:
            LOGGER.info('Output directory %s is created' % output_folder)
    # copy the flatten csv to the Data Factory output folder
    docker_csv_path = '/tmp/%s' % csv_name
    get_from(docker_csv_path, output_folder, container)
    LOGGER.info('Flat csv is saved in %s' % output_folder)


def anonymize_db(output_folder, csv_name, container, config):
    """Anonymize the i2b2 database & exports in a flat csv"""
    db_user = config['db_docker']['postgres_user']
    i2b2_source = config['db_docker']['harmonize_db']
    i2b2_anonym = config['db_docker']['anonymized_db']
    anonym_sql = config['anonymization']['anonymization_sql']
    pivoting_sql = config['anonymization']['strategy']['simple']
    anonymization_folder = os.path.abspath('./anonymization-4-federation')
    # drop the existing anonymized db and create a new one
    cmd_drop_db = 'psql -U %s -d postgres -c "DROP DATABASE IF EXIST %s;"' % (db_user,
                                                                              i2b2_anonym)
    LOGGER.info('Dropping previous anonymized i2b2 database')
    container.exec_run(cmd_drop_db)
    cmd_create_db = 'psql -U %s -d postgres -c "CREATE DATABASE %s WITH TEMPLATE %s;"' % (db_user, i2b2_anonym, i2b2_source)
    LOGGER.info('Copying i2b2 harmonized db')
    container.exec_run(cmd_create_db)
    # copy the anonymization sql to the postgres container
    sql_script_path = os.path.join(anonymization_folder, anonym_sql)
    copy_to(sql_script_path, '/tmp/', container)
    # run the anonymization sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user,
                                                  i2b2_anonym,
                                                  anonym_sql)
    LOGGER.info('Excecuting anonymization sql script...')
    container.exec_run(cmd_sql)
    export_csv(output_folder, csv_name, pivoting_sql, container, config)




# GET DATAFACTORY CONFIGURATION
with open('config.json') as json_data_file:
    config = json.load(json_data_file)

# EHR folders
ehr_source_root = os.path.abspath(config['mipmap']['input_folders']['ehr'])
dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])
output_root = os.path.abspath(config['flatening']['output_folder'])
preprocess_root = os.path.abspath(config['mipmap']['preprocess']['root'])
capture_root = os.path.abspath(config['mipmap']['capture']['root'])
harmonize_root = os.path.abspath(config['mipmap']['harmonize']['root'])
export = os.path.abspath(config['sql_scripts_folder'])

# IMAGING etl folders (output of imaging pipeline)
imaging_source_root = os.path.abspath(config['mipmap']['input_folders']['imaging'])
imaging_mapping_root = os.path.abspath(config['mipmap']['imaging']['root'])

# ANONYMIZATION
anonym_output_root = os.path.abspath(config['anonymization']['output_folder'])

# POSTGRES DOCKER CONTAINER
container_name = config['db_docker']['container_name']

client = docker.from_env()
try:
    pg_container = client.containers.get(container_name)
    LOGGER.info('Found postgres container named %s' % container_name)
except:
    LOGGER.warning('Unable to find db container %s' % container_name)
    exit()


def main():
    parser = argparse.ArgumentParser(description='EHR datafactory cli')
    parser.add_argument('step', choices=['preprocess', 'capture',
                                         'harmonize', 'export',
                                         'imaging', 'anonymize'],
                        help='select datafactory step')
    parser.add_argument('-m', '--mode', choices=['csv', 'db'],
                         help='anonymization file method')
    parser.add_argument('-s', '--source', type=str,
                        help='input folder for ehr csv file')
    parser.add_argument('-c', '--config', type=str,
                        help='folder containing step\'s configuration files')
    parser.add_argument('-o', '--output', type=str,
                        help='DF output folder for the flat csv')

    args = parser.parse_args(sys.argv[1:])
    if args.step == 'preprocess':
        source_folder = os.path.join(ehr_source_root, args.source)
        config_folder = os.path.join(preprocess_root, args.config)
        run_docker_compose(source_folder, config_folder, dbprop_folder)
    elif args.step == 'capture':
        source_folder = os.path.join(ehr_source_root, args.source)
        config_folder = os.path.join(capture_root, args.config)
        run_docker_compose(source_folder, config_folder, dbprop_folder)
    elif args.step == 'harmonize':
        source_folder = ehr_source_root
        config_folder = os.path.join(harmonize_root, args.config)
        run_docker_compose(source_folder, config_folder, dbprop_folder)
    elif args.step == 'imaging':
        source_folder = os.path.join(imaging_source_root, args.source)
        config_folder = imaging_mapping_root
        # process the volume.csv and split the subjectcode column into 
        # PATIENT_ID and VISIT_ID columns, creates the volume_df.csv 
        vfilename = config['mipmap']['imaging']['mapping']['input_files'][0]
        pfilename = config['mipmap']['imaging']['mapping']['processed_files'][0]
        inputpath = os.path.join(source_folder, vfilename)
        outputpath = os.path.join(source_folder, pfilename)
        LOGGER.info('Spliting \"subjectcode\" column to \"PATIENT_ID\" & \"VISIT_ID\"')
        split_subjectcode(inputpath, outputpath)
        LOGGER.info('New file stored in %s' % outputpath)
        run_docker_compose(source_folder, config_folder, dbprop_folder)
    elif args.step == 'anonymize':
        flat_anonym_csv = config['anonymization']['anonymized_csv_name']
        output_folder = os.path.join(anonym_output_root, args.output)
        if args.mode == 'db':
            LOGGER.info('i2b2 db anonymization mode')
            anonymize_db(output_folder, flat_anonym_csv, pg_container, config)
        elif args.mode == 'csv':
            LOGGER.info('csv anonymization mode')
            flat_csv_name = config['flatening']['export_csv_name']
            source_path = os.path.join(output_root, args.source, flat_csv_name)
            output_path = os.path.join(output_folder, flat_anonym_csv)
            anonymize_csv(source_path, output_path)
            LOGGER.info('Anonymized csv is saved in %s' % output_path)
        else:
            LOGGER.warning('Please define anonymization mode, see -m keyword')
    elif args.step == 'export':
        flat_csv_name = config['flatening']['export_csv_name']
        pivoting_sql = config['flatening']['strategy']['6months']
        LOGGER.info('Selected merging strategy is the 6 months time window')
        output_folder = os.path.join(output_root, args.output)
        export_csv(output_folder, flat_csv_name, pivoting_sql, pg_container, config)

if __name__ == '__main__':
    main()
