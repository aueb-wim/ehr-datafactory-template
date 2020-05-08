import os
import collections
import docker
import click
from helpers_docker import run_docker_compose, anonymize_db, extract2csv
from helpers_docker import sql_export_csv, anonymize_csv_wrapper
from helpers_imaging import split_subjectcode
from logger import LOGGER

DbConfig = collections.namedtuple('DbConfig',
                                  'container, port, user, pwd')


def mri_wrapper(ctx, input_folder, from_loris=False):

    # get config.json
    config = ctx.obj['cfgjson']

    # get the folders from config.json
    script_parallel_path = os.path.abspath('./mri_run_parallel')
    script_merge_path = os.path.abspath('./mri_output_merge')

    mri_raw_root = os.path.abspath(config['mri']['input_folders']['nifti']['raw'])
    mri_raw_folder = os.path.join(mri_raw_root, input_folder)

    mri_input_root = os.path.abspath(config['mri']['input_folders']['nifti']['organized'])
    mri_input_folder = os.path.join(mri_input_root, input_folder)

    imaging_root = os.path.abspath(config['mipmap']['input_folder']['imaging'])
    imaging_source_path = os.path.join(imaging_root, input_folder)

    mri_output_spm12_root = config['mri']['output_folders']['spm12']
    mri_output_spm12_folder = os.path.join(mri_output_spm12_root, input_folder)

    if not from_loris:
        # Reorganize mri files
        LOGGER.info('Reorganizing nifti files in folder %s' % mri_input_folder)
        run_cmd = 'python2 mri_nifti_reorganize/organizer.py %s %s' % (mri_raw_folder, mri_input_folder)
        os.system(run_cmd)
    else:
        LOGGER.info('Skipping NIFTI reorganization step, files already organized by LORIS-for-MIP')

    # run matlab spm12 script

    LOGGER.info('Running spm12 pipeline...')
    LOGGER.info('Storing output files in %s' % mri_output_spm12_folder)
    os.chdir(script_parallel_path)
    run_cmd = 'python2 mri_parallel_preprocessing.py %s %s' % (mri_input_folder, mri_output_spm12_folder)
    LOGGER.info('Executing...%s' % run_cmd)
    os.system(run_cmd)
    # merge the output into one csv
    os.chdir(script_merge_path)
    run_cmd = 'python2 merge.py %s %s' % (mri_output_spm12_folder, imaging_source_path)
    LOGGER.info('Merging spm12 output pipeline into single csv file in folder %s' % imaging_source_path)
    os.system(run_cmd)



def export_flat_csv(ctx, output_folder, strategy, local, dataset, csv_name=None):

    # get config.json
    config = ctx.obj['cfgjson']

    # get the i2b2 capture, harmonized db names from config.json
    if local:
        i2b2_db = config['db_docker']['capture_db']
    else:
        i2b2_db = config['db_docker']['harmonize_db']

    # extract the flat csv
    extract2csv(ctx, i2b2_db, output_folder, strategy, dataset,
                csv_name=csv_name)

    click.echo('Flat csv is created in output folder: %s' % output_folder)


 
def ehr_preprocess(ctx, input_folder, config_folder):
    """
    Creates the auxilary files for the capture DF EHR step.
    """

    pre_capture(ctx, 'preprocess', input_folder, config_folder)



def ehr_capture(ctx, input_folder, config_folder):
    """
    Ingest EHR data into capture i2b2.
    """

    pre_capture(ctx, 'capture', input_folder, config_folder)


def ehr_harmonize(ctx, config_folder):
    """
    Ingests and harmonizes EHR data from capture to harmonize i2b2.
    """

    # load root folders from config json
    config = ctx.obj['cfgjson']
    input_root = os.path.abspath(config['mipmap']['input_folders']['ehr'])
    config_root = os.path.abspath(config['mipmap']['harmonize']['root'])
    dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])

    # Get the configuration folder full path and check if exists
    config_path = os.path.join(config_root, config_folder)
    dir_g = os.listdir(config_path)
    if len(dir_g) == 0:
        click.echo('Configuration folder is empty')
        exit()

    run_docker_compose(input_root, config_path, dbprop_folder)



def ingest_imaging(ctx, input_folder):
    """
    Arguments
    input_folder: folder name of the which contains volumes.csv
                  and mri_visits.csv files
    """
    # load root folders from config json
    config = ctx.obj['cfgjson']
    input_root = os.path.abspath(config['mipmap']['input_folders']['imaging'])
    config_path = os.path.abspath(config['mipmap']['imaging']['root'])
    dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])

    input_path = os.path.join(input_root, input_folder)
    click.echo('Imaging Input folder: %s' % input_path)
    dir_f = os.listdir(input_path)
    if len(dir_f) == 0:
        click.echo('Imaging input folder is empty!')
        exit()

    run_docker_compose(input_path, config_path, dbprop_folder)



def export_anonymized_db(ctx, output_folder, hash_function, strategy, dataset, csv_name=None):

    # load config.json and db configuration
    config = ctx.obj['cfgjson']
    dbconfig = ctx.obj['dbconfig']

    # get the filename of anonymization sql script from config.json
    anonym_sql = config['anonymization']['anonymization_sql']

    # i2b2 db to be anonymized and anomymized db
    i2b2_anonym = config['db_docker']['anonymized_db']
    i2b2_harm = config['db_docker']['harmonize_db']

    anonymize_db(i2b2_harm, i2b2_anonym, anonym_sql,
                 hash_function, dbconfig)

    extract2csv(ctx, i2b2_anonym, output_folder, strategy, dataset,
                csv_name=csv_name, anonymized=True)

    click.echo('Anonymized csv is created in output folder: %s' % output_folder)



def export_anonymized_csv(ctx, input_path, output_folder, hash_function, csv_name, dataset):

    # load config.json and db configuration
    config = ctx.obj['cfgjson']

    # get full path anonymization folder from config.json
    output_root = os.path.abspath(config['anonymization']['output_folder'])
    output_path = os.path.join(output_root, output_folder)

    # anonymized the csv
    anonymize_csv_wrapper(input_path, output_path,
                          csv_name, hash_function, dataset)


def pre_capture(ctx, step, input_folder, config_folder):

    # load root folders from config json
    config = ctx.obj['cfgjson']
    input_root = os.path.abspath(config['mipmap']['input_folders']['ehr'])
    config_root = os.path.abspath(config['mipmap'][step]['root'])
    dbprop_folder = os.path.abspath(config['mipmap']['dbproperties'])

    # Get the input folder full path and check if exists
    input_path = os.path.join(input_root, input_folder)
    click.echo('EHR input folder: %s' % input_path)
    dir_f = os.listdir(input_path)
    if len(dir_f) == 0:
        click.echo('Input folder is empty!')
        exit()

    # Get the configuration folder full path and check if exists
    config_path = os.path.join(config_root, config_folder)
    dir_g = os.listdir(config_path)
    if len(dir_g) == 0:
        click.echo('Configuration folder is empty')
        exit()

    run_docker_compose(input_path, config_path, dbprop_folder)
