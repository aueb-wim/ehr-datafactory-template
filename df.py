#!/usr/bin/env python3
import os
import json
import collections
import docker
import click
import dfpipelines
from dfpipelines import DbConfig

from logger import LOGGER


# GET THE CONFIGURATIONS AND INPUT FOLDERS
with open('config.json') as json_data_file:
    CONFIG = json.load(json_data_file)


def getfolders(rootfolder):
    """Returns the subfolders in a folder."""
    return [name for name in os.listdir(rootfolder)
            if os.path.isdir(os.path.join(rootfolder, name))]


EHR_INPUT_ROOT = CONFIG['mipmap']['input_folders']['ehr']
IMG_INPUT_ROOT = CONFIG['mipmap']['input_folders']['imaging']
MRI_RAW_ROOT = CONFIG['mri']['input_folders']['nifti']['raw']
CFG_PRE_ROOT = CONFIG['mipmap']['preprocess']['root']
CFG_CAP_ROOT = CONFIG['mipmap']['capture']['root']
CFG_HAR_ROOT = CONFIG['mipmap']['harmonize']['root']


EHR_INPUT = getfolders(EHR_INPUT_ROOT)
IMG_INPUT = getfolders(IMG_INPUT_ROOT)
MRI_INPUT = getfolders(MRI_RAW_ROOT)

CFG_PRE = getfolders(CFG_PRE_ROOT)
CFG_CAP = getfolders(CFG_CAP_ROOT)
CFG_HAR = getfolders(CFG_HAR_ROOT)


FLAT_STRATEGIES = list(CONFIG['flattening']['strategy'].keys())
FLAT_STRATEGIES_ANON = list(CONFIG['anonymization']['strategy'].keys())

@click.group()
@click.pass_context
def main(ctx):
    # GET DATAFACTORY CONFIGURATION
    with open('config.json') as json_data_file:
        config = json.load(json_data_file)

    # test if postgres docker container is running
    container_name = config['db_docker']['container_name']
    client = docker.from_env(timeout=10800)

    try:
        pg_container = client.containers.get(container_name)
        LOGGER.info('Found postgres container: %s' % container_name)
    except:
        LOGGER.warning('Unable to find db container: %s' % container_name)
        exit()

    # docker db credentials
    dbconfig = DbConfig(pg_container,
                        config['db_docker']['postgres_port'],
                        config['db_docker']['postgres_user'],
                        config['db_docker']['postgres_pwd'])

    ctx.obj = {
        'cfgjson': config,
        'dbconfig': dbconfig,
    }


@main.group()
@click.pass_context
def ingest(ctx):
    pass


@main.group()
@click.pass_context
def anonymize(ctx):
    pass


@main.group()
@click.option('--input_folder', prompt=True,
              type=click.Choice(MRI_INPUT))
@click.option('--loris', is_flag=True)
def mri(ctx, input_folder, loris):
    dfpipelines.mri_wrapper(ctx, input_folder, from_loris=loris)


@main.command()
@click.argument('output_folder')
@click.option('-s', '--strategy', prompt=True,
              type=click.Choice(FLAT_STRATEGIES),
              help='Flattening strategy from i2b2 to csv')
@click.option('--local', is_flag=True)
@click.option('--csv_name',
              help='overrides the defalut strategy\'s flat csv name')
@click.option('-d', '--dataset', prompt=True,
              help='value in the final csv under the column \'Dataset\'')
@click.pass_context
def export(ctx, output_folder, strategy, local, csv_name, dataset):
    dfpipelines.export_flat_csv(ctx, output_folder, strategy, local, csv_name, dataset)


@ingest.group()
@click.pass_context
def ehr(ctx):
    pass

    
@ehr.command()
@click.option('--input_folder', prompt=True, type=click.Choice(EHR_INPUT),
              help='batch folder name that contains EHR csv files')
@click.option('--config_folder', prompt=True, type=click.Choice(CFG_PRE),
              help='config folder for preprosessing step')
@click.pass_context
def preprocess(ctx, input_folder, config_folder):
    """
    Creates the auxilary files for the capture DF EHR step.
    """
    dfpipelines.ehr_preprocess(ctx, input_folder, config_folder)


@ehr.command()
@click.option('--input_folder', prompt=True, type=click.Choice(EHR_INPUT),
              help='batch folder name that contains EHR csv files')
@click.option('--config_folder', prompt=True, type=click.Choice(CFG_CAP),
              help='config folder for capture step')
@click.pass_context
def capture(ctx, input_folder, config_folder):
    """
    Ingest EHR data into capture i2b2.
    """
    dfpipelines.ehr_capture(ctx, input_folder, config_folder)


@ehr.command()
@click.option('--config_folder', prompt=True, type=click.Choice(CFG_HAR),
              help='config folder for harmonization step')
@click.pass_context
def harmonize(ctx, config_folder):
    """
    Ingests and harmonizes EHR data from capture to harmonize i2b2.
    """
    dfpipelines.ehr_harmonize(ctx, config_folder)


@ingest.command()
@click.option('--input_folder', prompt=True, type=click.Choice(IMG_INPUT),
              help='batch folder with the output of imaging pipeline')
@click.pass_context
def imaging(ctx, input_folder):
    """
    Arguments
    input_folder: folder name of the which contains volumes.csv
                  and mri_visits.csv files
    """
    dfpipelines.ingest_imaging(ctx, input_folder)


@anonymize.command()
@click.argument('output_folder')
@click.option('-s', '--strategy', prompt=True,
              type=click.Choice(FLAT_STRATEGIES_ANON),
              help='Flattening strategy from i2b2 to csv')
@click.option('--hash_function', default='sha224',
              type=click.Choice(['md5', 'sha224']),
              help='Hashing function used for anonymization')
@click.option('--csv_name',
              help='overrides the defalut strategy\'s flat csv name')
@click.option('-d', '--dataset', prompt=True,
              help='value in the final csv under the column \'Dataset\'')
@click.pass_context
def db(ctx, output_folder, hash_function, strategy, csv_name, dataset):
    dfpipelines.export_anonymized_db(ctx, output_folder, hash_function, strategy, csv_name, dataset)


@anonymize.command()
@click.argument('input_path', type=click.Path(exists=True, file_okay=True))
@click.argument('output_folder')
@click.option('--csv_name', prompt=True,
              help='anonymized flat csv name')
@click.option('--hash_function', default='sha224',
              type=click.Choice(['md5', 'sha224']),
              help='Hashing function used for anonymization')
@click.option('-d', '--dataset', prompt=True,
              help='value in the final csv under the column \'Dataset\'')
@click.pass_context
def csv(ctx, input_path, output_folder, hash_function, csv_name, dataset):
    dfpipelines.export_anonymized_csv(ctx, input_path, output_folder, hash_function, csv_name, dataset)


@main.command()
@click.pass_context
def interactive(ctx):
    click.clear()
    click.echo('DataFactory EHR pipeline - interactive mode')
    click.confirm('Do you want to continue?', abort=True)

    # PREPROCESS 
    if click.confirm('Proceed with the EHR preprocess step?', abort=False):
        click.clear()
        inp_num = [str(x) for x in range(len(EHR_INPUT))]
        prompt_msg = 'Select input folder number: '
        prompt_msg = enum_options(prompt_msg, EHR_INPUT)
        choice = click.prompt(prompt_msg, type=click.Choice(inp_num))
        input_idx = int(choice)
        input_folder = EHR_INPUT[input_idx]

        click.clear()
        inp_num = [str(x) for x in range(len(CFG_PRE))]
        prompt_msg = 'Select configuration folder number: '
        prompt_msg = enum_options(prompt_msg, CFG_PRE)
        choice = click.prompt(prompt_msg, type=click.Choice(inp_num))
        input_idx = int(choice)
        config_folder = CFG_PRE[input_idx]

        dfpipelines.ehr_preprocess(ctx, input_folder, config_folder)
    
    # CAPTURE 
    if click.confirm('Proceed with the EHR capture step?', abort=False):
        click.clear()
        inp_num = [str(x) for x in range(len(EHR_INPUT))]
        prompt_msg = 'Select input folder number: '
        prompt_msg = enum_options(prompt_msg, EHR_INPUT)
        choice = click.prompt(prompt_msg, type=click.Choice(inp_num))
        input_idx = int(choice)
        input_folder = EHR_INPUT[input_idx]

        click.clear()
        inp_num = [str(x) for x in range(len(CFG_CAP))]
        prompt_msg = 'Select configuration folder number: '
        prompt_msg = enum_options(prompt_msg, CFG_CAP)
        choice = click.prompt(prompt_msg, type=click.Choice(inp_num))
        input_idx = int(choice)
        config_folder = CFG_CAP[input_idx]

        dfpipelines.ehr_capture(ctx, input_folder, config_folder)

    # HARMONIZE
    if click.confirm('Proceed with the EHR harmonization step?', abort=False):
        click.clear()
        inp_num = [str(x) for x in range(len(CFG_HAR))]
        prompt_msg = 'Select configuration folder number: '
        prompt_msg = enum_options(prompt_msg, CFG_HAR)
        choice = click.prompt(prompt_msg, type=click.Choice(inp_num))
        input_idx = int(choice)
        config_folder = CFG_HAR[input_idx]

        dfpipelines.ehr_harmonize(ctx, config_folder)

    # EXPORT
    if click.confirm('Proceed with the export step?', abort=False):
        click.clear()
        if click.confirm('Do you want anonymized export?', abort=False):
            output_folder = click.prompt('Enter name for the output folder: ', type=str)
            hash_function = click.prompt('Select flattening strategy: ', click.Choice(['md5', 'sha224']))
            flat_strategy= click.prompt('Select flattening strategy: ', click.Choice(FLAT_STRATEGIES_ANON))
            dataset = click.prompt('Enter dataset name: ', type=str)
            
            dfpipelines.export_anonymized_db(ctx, output_folder, hash_function, flat_strategy, dataset)

        else:
            output_folder = click.prompt('Enter name for the output folder: ', type=str)
            hash_function = click.prompt('Select flattening strategy: ', click.Choice(['md5', 'sha224']))
            flat_strategy= click.prompt('Select flattening strategy: ', click.Choice(FLAT_STRATEGIES))
            dataset = click.prompt('Enter dataset name: ', type=str)
            local = click.confirm('Use local hospital variables instead of CDE variables', abort=False)

            dfpipelines.export_flat_csv(ctx, output_folder, flat_strategy, local, dataset)
            


        
            
def enum_options(prompt_msg, options):
    opt_str = ''
    for num, opt in enumerate(options):
        s = '%d: %s \n' % (num, opt)
        opt_str += s
    
    return opt_str + prompt_msg


if __name__ == '__main__':
    main()
