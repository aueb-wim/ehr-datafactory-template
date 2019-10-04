#!/usr/bin/env python3
import os
import sys
import argparse
import json
import tarfile
import logging
import docker


# Get the DataFactory configuration
with open('config.json') as json_data_file:
    config = json.load(json_data_file)

# DataFactory EHR folders
source_root = os.path.abspath(config['mipmap']['input_folders']['ehr'])
dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])
output_root = os.path.abspath(config['flatening']['output_folder'])
preprocess_root = os.path.abspath(config['mipmap']['preprocess'])
capture_root = os.path.abspath(config['mipmap']['capture'])
harmonize_root = os.path.abspath(config['mipmap']['harmonize'])
export = os.path.abspath(config['sql_script_folder'])



# imaging etl folders (output of imaging pipeline)
image_root = os.path.abspath(config['mipmap']['input_folders']['imaging'])

# anonymization files
anonym_output_root = config['anonymization']['output_folder']

# get postgres container name
container_name = config['db_docker']['container_name']

client = docker.from_env()
try:
    pg_container = client.containers.get(container_name)
except:
    logging.warning('Unable to find db container %s' % container_name)
    exit()
# set enviroment variables
os.environ['mipmap_pgproperties'] = dbprop_folder


def run_docker_compose(cnfg_folder, source=None, imaging=False):
    if source:
        if imaging:
            source_path = os.path.join(image_root, source)
        else:
            source_path = os.path.join(source_root, source)
        os.environ['mipmap_source'] = source_path
    os.environ['mipmap_map'] = cnfg_folder
    os.system('docker-compose run mipmap_etl')


def copy_to(src, dst):
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(src, arcname=os.path.basename(src))
    finally:
        tar.close()
    data = open(src + '.tar', 'rb').read()
    pg_container.put_archive(os.path.dirname(dst), data)
    os.remove(src + '.tar')


def get_from(src, dst):
    bits, stats = pg_container.get_archive(src)
    tmp_tar_path = os.path.join(dst, 'tmp.tar')
    f = open(tmp_tar_path, 'wb')
    for chunk in bits:
        f.write(chunk)
    f.close
    tar = tarfile.open(tmp_tar_path)
    tar.extractall(dst)
    os.remove(tmp_tar_path)


def export_csv(output_folder, csv_name, sql_script):
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
    copy_to(sql_script_path, '/tmp/')
    print(os.path.abspath(os.path.dirname(__file__)))
    # run the sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user, i2b2_name, sql_script)
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
    get_from(docker_csv_path, output_folder)


def anonymize_db(output_folder, csv_name):
    db_user = config['db_docker']['postgres_user']
    i2b2_source = config['db_docker']['harmonize_db']
    i2b2_anonym = config['db_docker']['anonymized_db']
    anonym_sql = config['anonymization']['anonymization_sql']
    pivoting_sql = config['anonymization']['strategy']['simple']
    # drop the existing anonymized db and create a new one
    cmd_drop_db = 'psql -U %s -d postgres -c "DROP DATABASE IF EXIST %s;"' % (db_user, i2b2_anonym)
    pg_container.exec_run(cmd_drop_db)
    cmd_create_db = 'psql -U %s -d postgres -c "CREATE DATABASE %s WITH TEMPLATE %s;"' % (db_user, i2b2_anonym, i2b2_source)
    pg_container.exec_run(cmd_create_db)
    # copy the anonymization sql to the postgres container
    sql_script_path = os.path.join(export, anonym_sql)
    print(sql_script_path)
    copy_to(sql_script_path, '/tmp/')
    # run the anonymization sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user, i2b2_anonym, anonym_sql)
    pg_container.exec_run(cmd_sql)
    export_csv(output_folder, csv_name, pivoting_sql)


# run_docker_compose('./preprocess_step')
# test_output = output_root+'/'
# export_csv('./output/')
# copy_to('./export_step/pivot_i2b2_MinDate_NEW19_a.sql', '/tmp/sql_script.sql')


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
        config_folder = os.path.join(preprocess_root, args.config)
        run_docker_compose(config_folder, source=args.source)
    elif args.step == 'capture':
        config_folder = os.path.join(capture_root, args.config)
        run_docker_compose(config_folder, source=args.source)
    elif args.step == 'harmonize':
        config_folder = os.path.join(harmonize_root, args.config)
        run_docker_compose(config_folder)
    elif args.step == 'imaging':
        image_mapping = os.path.abspath(config['mipmap']['imaging'])
        run_docker_compose(image_mapping, source=args.source,
                           imaging=True)
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