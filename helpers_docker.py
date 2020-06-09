import tarfile
import os
import csv
import sys
import collections
from logger import LOGGER
#sys.path.insert(1, os.path.abspath('./anonymization'))
from anonymization.anonymize_csv import anonymize_csv


CsvStrategy = collections.namedtuple('CsvStrategy',
                                     'sql_folder, sql_file, csv_name')


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


def sql_export_csv(output_folder, strategy,
                   dbname, dbconfig, dataset):
    """Exports a flat csv from i2b2"""
    db_user = dbconfig.user
    i2b2_name = dbname
    sql_script = strategy.sql_file
    sql_folder = strategy.sql_folder
    container = dbconfig.container
    LOGGER.info('Performing EHR DataFactory export step')
    # try to delete any existing flattened csv in postgres container
    try:
        cmd_rm_csv = 'rm -rf /tmp/%s' % strategy.csv_name
        container.exec_run(cmd_rm_csv, stream=True)
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
    output = container.exec_run(cmd_sql, stream=True)
    for out in output:
        LOGGER.info(out)

    # check if the output folder exist, create it otherwise
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
        except OSError:
            LOGGER.warning('Creation of the output directory %s failed' % output_folder)
        else:
            LOGGER.info('Output directory %s is created' % output_folder)
    # copy the flatten csv to the Data Factory output folder
    docker_csv_path = '/tmp/%s' % strategy.csv_name
    get_from(docker_csv_path, output_folder, container)
    csv_on_host_ = os.path.join(output_folder, strategy.csv_name)
    csv_temp = os.path.join(output_folder, 'temp.csv')
    os.rename(csv_on_host_, csv_temp)
    add_column_csv(csv_temp, csv_on_host_, 'Dataset', dataset)
    os.remove(csv_temp)


def anonymize_db(i2b2_source, i2b2_anonym, anonym_sql,
                 hash_function, dbconfig):
    """Anonymize the i2b2 database & exports in a flat csv
    Arguments:
    :param i2b2_source: name of the database to be anonymized
    :param i2b2_anon: name of the anonymized i2b2 database
    :anonym_sql: filename of the anonymization sql script
    :hash_function: hash function for anonymization
    :dbconfig: a named tuple config for db connection
    """
    container = dbconfig.container
    db_user = dbconfig.user
    anonymization_folder = os.path.abspath('./anonymization')
    # drop the existing anonymized db and create a new one
    cmd_drop_db = 'psql -U %s -d postgres -c "DROP DATABASE IF EXISTS %s;"' % (db_user,
                                                                              i2b2_anonym)
    LOGGER.info('Dropping previous anonymized i2b2 database')
    container.exec_run(cmd_drop_db, stream=True)
    cmd_create_db = 'psql -U %s -d postgres -c "CREATE DATABASE %s WITH TEMPLATE %s;"' % (db_user, i2b2_anonym, i2b2_source)
    LOGGER.info('Copying i2b2 harmonized db')
    container.exec_run(cmd_create_db, stream=True)
    # copy the anonymization sql to the postgres container
    sql_script_path = os.path.join(anonymization_folder, anonym_sql)
    copy_to(sql_script_path, '/tmp/', container)

    # command for the anonymization function though sql script
    cmd_sql = 'psql -q -U %s -d %s -f /tmp/%s' % (db_user,
                                                  i2b2_anonym,
                                                  anonym_sql)
    container.exec_run(cmd_sql,stream=True)
    # run the anonymization function
    LOGGER.info('Executing anonymization sql script...')
    hash_function = "'" + hash_function + "'"
    cmd_run = 'psql -U %s -d %s -c "SELECT anonymize_db(%s);"' % (db_user,
                                                                   i2b2_anonym,
                                                                   hash_function)
    container.exec_run(cmd_run, stream=True)
    LOGGER.info('Anonymized i2b2 db is created.')


def anonymize_csv_wrapper(input_csv, output_folder, anon_csv_name,
                          hash_df, dataset):
    if not os.path.exists(output_folder):
        try:
            os.makedirs(output_folder)
            LOGGER.info('Output directory %s is created' % output_folder)
        except OSError:
            LOGGER.warning('Creation of the output directory %s failed' % output_folder)
    if hash_df == 'sha224':
        hash_method = 'sha3'
    elif hash_df == 'md5':
        hash_method = 'md5'
    output_path = os.path.join(output_folder, anon_csv_name)
    anonymize_csv(input_csv, output_path, columns=[0], method=hash_method)
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


def extract2csv(ctx, i2b2_db, output_folder,
                strategy, dataset, csv_name=None, anonymized=False):
    """
    Creates a flat csv from i2b2 db according to the given strategy. 
    """
    # load root folders from config.json
    config = ctx.obj['cfgjson']
    dbconfig = ctx.obj['dbconfig']

    sql_folder = config['sql_scripts_folder']

    # get the folders and scripts for flattening or for anoymization
    if anonymized:
        flag = 'anonymization'
    else:
        flag = 'flattening'

    output_root = config[flag]['output_folder']

    flat_csv_name = config[flag]['strategy'][strategy]['csv']
    pivoting_sql = config[flag]['strategy'][strategy]['sql']

    csv_strategy = CsvStrategy(sql_folder, pivoting_sql, flat_csv_name)
    LOGGER.info('Selected flattening strategy: %s' % strategy)
    output_path = os.path.join(output_root, output_folder)

    # export the csv file into the output_folder
    sql_export_csv(output_path, csv_strategy,
                   i2b2_db, dbconfig, dataset)

    # overide the csv name if the option is given
    if csv_name:
        default_name = os.path.join(output_path, csv_strategy.csv_name)
        new_name = os.path.join(output_folder, csv_name)
        os.rename(default_name, new_name)
