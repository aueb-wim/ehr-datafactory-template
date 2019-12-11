import tarfile
import os
import csv
import sys
from logger import LOGGER
sys.path.insert(1, os.path.abspath('./anonymization'))
from anonymize_csv import anonymize_csv

def copy_to(src, dst, container):
    """Copies file from host to postgres container
    Arguments:
    :src: filepath in host filesystem
    :dst: filepath in the container's filesystem
    :container: a docker py container object"""
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(src, arcname=os.path.basename(src))
    finally:
        tar.close()
    data = open(src + '.tar', 'rb').read()
    container.put_archive(os.path.dirname(dst), data)
    os.remove(src + '.tar')


def get_from(src, dst, container):
    """Copies file from postgres container to host
    Arguments:
    :src: filepath in container's filesystem
    :dst: filepath in the host's filesystem
    :container: a docker py container object"""
    bits, stats = container.get_archive(src)
    tmp_tar_path = os.path.join(dst, 'tmp.tar')
    with open(tmp_tar_path, 'wb') as tmptar:
        for chunk in bits:
            tmptar.write(chunk)
    tar = tarfile.open(tmp_tar_path)
    tar.extractall(dst)
    os.remove(tmp_tar_path)


def run_docker_compose(source_folder, cnfg_folder, dbprop_folder):
    os.environ['mipmap_pgproperties'] = dbprop_folder
    LOGGER.info('Mounting %s as source folder' % source_folder)
    os.environ['mipmap_source'] = source_folder
    LOGGER.info('Mounting %s as mapping folder' % cnfg_folder)
    os.environ['mipmap_map'] = cnfg_folder
    LOGGER.info('Running... docker-compose up mipmap_etl')
    os.system('docker-compose up mipmap_etl')
    LOGGER.info('Removing mipmap container')
    os.system('docker rm mipmap')


def export_csv(output_folder, csv_name,
               sql_script, container, config, dataset):
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
    csv_path = os.path.join(output_folder, csv_name)
    csv_temp = os.path.join(output_folder, 'temp.csv')
    os.rename(csv_path, csv_temp)
    add_column_csv(csv_temp, csv_path, 'Dataset', dataset)
    os.remove(csv_temp)
    LOGGER.info('Flat csv is saved in %s' % output_folder)


def anonymize_db(output_folder, anon_csv_name, strategy,
                 container, config, dataset):
    """Anonymize the i2b2 database & exports in a flat csv"""
    db_user = config['db_docker']['postgres_user']
    i2b2_source = config['db_docker']['harmonize_db']
    i2b2_anonym = config['db_docker']['anonymized_db']
    anonym_sql = config['anonymization']['anonymization_sql']
    pivoting_sql = config['anonymization']['strategy'][strategy]
    anonymization_folder = os.path.abspath('./anonymization')
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
    export_csv(output_folder, anon_csv_name, pivoting_sql,
               container, config, dataset)


def anonymize_csv_wrapper(input_csv, output_folder, anon_csv_name, dataset):
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            LOGGER.info('Output directory %s is created' % output_folder)
        except OSError:
            LOGGER.warning('Creation of the output directory %s failed' % output_folder)
    output_path = os.path.join(output_folder, anon_csv_name)
    anonymize_csv(input_csv, output_path, columns=[0], method='sha3')
    LOGGER.info('Anonymized csv is saved in %s' % output_path)


def add_column_csv(csv_path_in, csv_path_out, column, value):
    """Add a column in a csv with the given value in all rows.
    The function check if the columns exists, in this case replaces
    the values in all rows with the given value.

    Arguments:
    :param csv_path_in: the path of the input csv file
    :param csv_path_out: the path of the output csv file
    :param column: the column name
    :param value: the column value
    """
    with open(csv_path_in, 'r') as csv_file_in:
        with open(csv_path_out, 'w') as csv_file_out:
            csv_reader = csv.reader(csv_file_in)
            csv_writer = csv.writer(csv_file_out)
            row1 = next(csv_reader)
            if column in row1:
                index = row1.index(column)
                append = False
            else:
                index = len(row1)
                row1.append(column)
                append = True
            csv_writer.writerow(row1)
            if append:
                for row in csv_reader:
                    row.append(value)
                    csv_writer.writerow(row)
            else:
                for row in csv_reader:
                    row[index] = value
                    csv_writer.writerow(row)
