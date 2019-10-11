#!/usr/bin/env python3
import os
import sys
import argparse
import json
import tarfile
import logging
import docker
from helpers_docker import get_from, copy_to
from helpers_imaging import split_subjectcode


def run_docker_compose(source_folder, cnfg_folder, dbprop_folder):
    os.environ['mipmap_pgproperties'] = dbprop_folder
    os.environ['mipmap_source'] = source_folder
    os.environ['mipmap_map'] = cnfg_folder
    os.system('docker-compose up mipmap_etl')
    os.system('docker rm mipmap')


def export_csv(output_folder, csv_name, sql_script):
    """Exports a flat csv from i2b2"""
    db_user = config['db_docker']['postgres_user']
    i2b2_name = config['db_docker']['harmonize_db']
    logging.info('Performing EHR DataFactory export step')
    # try to delete any existing flattened csv in postgres container
    try:
        cmd_rm_csv = 'rm -rf /tmp/%s' % csv_name
        pg_container.exec_run(cmd_rm_csv)
    except:
        pass
    # Copy the sql script to the postgres container
    sql_script_path = os.path.join(export, sql_script)
    copy_to(sql_script_path, '/tmp/', pg_container)
    print(os.path.abspath(os.path.dirname(__file__)))
    # run the sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user,
                                                  i2b2_name,
                                                  sql_script)
    pg_container.exec_run(cmd_sql)
    # check if the output folder exist, create it otherwise
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
        except OSError:
            print('Creation of the output directory %s failed' % output_folder)
        else:
            print('Output directory %s is created' % output_folder)
    # copy the flatten csv to the Data Factory output folder
    docker_csv_path = '/tmp/%s' % csv_name
    get_from(docker_csv_path, output_folder, pg_container)


def anonymize_db(output_folder, csv_name):
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
    pg_container.exec_run(cmd_drop_db)
    cmd_create_db = 'psql -U %s -d postgres -c "CREATE DATABASE %s WITH TEMPLATE %s;"' % (db_user, i2b2_anonym, i2b2_source)
    pg_container.exec_run(cmd_create_db)
    # copy the anonymization sql to the postgres container
    sql_script_path = os.path.join(anonymization_folder, anonym_sql)
    print(sql_script_path)
    copy_to(sql_script_path, '/tmp/', pg_container)
    # run the anonymization sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user,
                                                  i2b2_anonym,
                                                  anonym_sql)
    pg_container.exec_run(cmd_sql)
    export_csv(output_folder, csv_name, pivoting_sql)


# run_docker_compose('./preprocess_step')
# test_output = output_root+'/'
# export_csv('./output/')
# copy_to('./export_step/pivot_i2b2_MinDate_NEW19_a.sql', '/tmp/sql_script.sql')

# GET DATAFACTORY CONFIGURATION
with open('config.json') as json_data_file:
    config = json.load(json_data_file)

# EHR folders
ehr_source_root = os.path.abspath(config['mipmap']['input_folders']['ehr'])
dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])
print(dbprop_folder)
output_root = os.path.abspath(config['flatening']['output_folder'])
preprocess_root = os.path.abspath(config['mipmap']['preprocess']['root'])
capture_root = os.path.abspath(config['mipmap']['capture']['root'])
harmonize_root = os.path.abspath(config['mipmap']['harmonize']['root'])
export = os.path.abspath(config['sql_script_folder'])

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
except:
    logging.warning('Unable to find db container %s' % container_name)
    exit()


def main():
    parser = argparse.ArgumentParser(description='EHR datafactory cli')
    parser.add_argument('step', choices=['preprocess', 'capture',
                                         'harmonize', 'export',
                                         'imaging', 'anonymize'],
                        help='select datafactory step')
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
        config_folder = os.path.join(imaging_mapping_root, args.config)
        # process the volume.csv and split the subjectcode column into 
        # PATIENT_ID and VISIT_ID columns, creates the volume_df.csv 
        vfilename = config['mipmap']['imaging']['mapping']['input_files'][0]
        pfilename = config['mipmap']['imaging']['mapping']['processed_files'][0]
        inputpath = os.path.join(source_folder, vfilename)
        outputpath = os.path.join(source_folder, pfilename)
        split_subjectcode(inputpath, outputpath)
        run_docker_compose(source_folder, config_folder, dbprop_folder)
    elif args.step == 'anonymize':
        flat_anonym_csv = config['anonymization']['anonymized_csv_name']
        output_folder = os.path.join(anonym_output_root, args.output)
        anonymize_db(output_folder, flat_anonym_csv)
    elif args.step == 'export':
        flat_csv_name = config['flatening']['export_csv_name']
        pivoting_sql = config['flatening']['strategy']['simple']
        output_folder = os.path.join(output_root, args.output)
        export_csv(output_folder, flat_csv_name, pivoting_sql)

if __name__ == '__main__':
    main()
