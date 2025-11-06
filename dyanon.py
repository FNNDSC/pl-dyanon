#!/usr/bin/env python

from pathlib import Path
from argparse import ArgumentParser, Namespace, ArgumentDefaultsHelpFormatter
from pflog import pflog
from loguru import logger
from chris_plugin import chris_plugin, PathMapper
import pandas as pd
import json
import itertools
from collections import ChainMap
from chrisClient import ChrisClient
import pfdcm
import sys
import time
import os
import concurrent.futures
import asyncio

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

__version__ = '1.1.4'

DISPLAY_TITLE = r"""
       _           _                               
      | |         | |                              
 _ __ | |______ __| |_   _  __ _ _ __   ___  _ __  
| '_ \| |______/ _` | | | |/ _` | '_ \ / _ \| '_ \ 
| |_) | |     | (_| | |_| | (_| | | | | (_) | | | |
| .__/|_|      \__,_|\__, |\__,_|_| |_|\___/|_| |_|
| |                   __/ |                        
|_|                  |___/                         
"""

parser = ArgumentParser(description='A ChRIS plugin to dynamically anonymize DICOMs in bulk',
                        formatter_class=ArgumentDefaultsHelpFormatter)

parser.add_argument(
    '-V', '--version',
    action='version',
    version=f'%(prog)s {__version__}'
)
parser.add_argument(
    "--pattern",
    default="**/*csv",
    help="""
            pattern for file names to include.
            Default is **/*csv.""",
)
parser.add_argument(
    "--pluginInstanceID",
    default="",
    help="plugin instance ID from which to start analysis",
)
parser.add_argument(
    "--CUBEurl",
    default="http://localhost:8000/api/v1/",
    help="CUBE URL. Please include api version in the url endpoint."
)
parser.add_argument(
    "--CUBEtoken",
    default="",
    help="CUBE/ChRIS user token"
)

parser.add_argument(
    "--maxThreads",
    default=4,
    help="max number of parallel threads"
)
parser.add_argument(
    '--orthancUrl',
    help='Orthanc server url. Please include api version in the url endpoint.',
    default='http://0.0.0.0:8042'
)

parser.add_argument(
    '--orthancUsername',
    help='Orthanc server username',
    default='orthanc'
)

parser.add_argument(
    '--orthancPassword',
    help='Orthanc server password',
    default='orthanc'
)

parser.add_argument(
    '--pushToRemote',
    help='Remote modality',
    default=''
)
parser.add_argument(
    "--thread",
    help="use threading to branch in parallel",
    dest="thread",
    action="store_true",
    default=False,
)
parser.add_argument(
    "--wait",
    help="wait for nodes to reach finished state",
    dest="wait",
    action="store_true",
    default=False,
)
parser.add_argument(
    '--PFDCMurl',
    default='',
    type=str,
    help='endpoint URL of pfdcm. Please include api version in the url endpoint.'
)
parser.add_argument(
    '--PACSname',
    default='MINICHRISORTHANC',
    type=str,
    help='name of the PACS'
)
parser.add_argument(
    '--recipients',
    default='',
    type=str,
    help='comma separated valid email recipient addresses'
)
parser.add_argument(
    '--SMTPServer',
    default='mailsmtp4.childrenshospital.org',
    type=str,
    help='valid email server'
)
parser.add_argument(
    '--preserveTags',
    default='',
    type=str,
    help='A stringified JSON of header tags from the original DICOM to keep'
)

# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser=parser,
    title='A dynamic anonymization ChRIS plugin',
    category='',  # ref. https://chrisstore.co/plugins
    min_memory_limit='100Mi',  # supported units: Mi, Gi
    min_cpu_limit='1000m',  # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit=0  # set min_gpu_limit=1 to enable GPU
)
@pflog.tel_logTime(
    event="dyanon", log="Dynamic Anonymization of DICOMs"
)
def main(options: Namespace, inputdir: Path, outputdir: Path):
    """
    *ChRIS* plugins usually have two positional arguments: an **input directory** containing
    input files and an **output directory** where to write output files. Command-line arguments
    are passed to this main method implicitly when ``main()`` is called below without parameters.

    :param options: non-positional arguments parsed by the parser given to @chris_plugin
    :param inputdir: directory containing (read-only) input files
    :param outputdir: directory where to write output files
    """

    print(DISPLAY_TITLE)

    log_file = os.path.join(options.outputdir, 'terminal.log')
    logger.add(log_file)
    LOG(f"Logs are stored in {log_file}")

    if not health_check(options): return

    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.pattern)
    for input_file, output_file in mapper:
        LOG(f"Reading input from {input_file}")
        df = pd.read_csv(input_file, dtype=str)
        l_job = create_query(df)
        if int(options.thread):
            with concurrent.futures.ThreadPoolExecutor(max_workers=int(options.maxThreads)) as executor:
                results: Iterator = executor.map(lambda t: register_and_anonymize(options, t, options.wait), l_job)

            # Wait for all tasks to complete
            # executor.shutdown(wait=True)
        else:
            for d_job in l_job:
                response = asyncio.run(register_and_anonymize(options, d_job))


if __name__ == '__main__':
    main()


async def register_and_anonymize(options: Namespace, d_job: dict, wait: bool = False):
    """
    1) Search through PACS for series and register in CUBE
    2) Run anonymize and push workflow on the registered series
    """
    d_job["pull"] = {
        "url": options.PFDCMurl,
        "pacs": options.PACSname
    }
    d_job["notify"] = {
        "recipients": options.recipients,
        "smtp_server": options.SMTPServer
    }
    d_job["push"] = {
        "url": options.orthancUrl,
        "username": options.orthancUsername,
        "password": options.orthancPassword,
        "aec": options.pushToRemote,
        "wait": wait
    }
    d_job["preserve"] = {
        "preserveTags": options.preserveTags
    }
    LOG(d_job)
    cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
    d_ret = await cube_con.anonymize(d_job, options.pluginInstanceID)


def health_check(options) -> bool:
    """
    check if connections to pfdcm and CUBE is valid
    """
    try:
        if not options.pluginInstanceID:
            options.pluginInstanceID = os.environ['CHRIS_PREV_PLG_INST_ID']
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # create connection object
        if not options.CUBEtoken:
            options.CUBEtoken = os.environ['CHRIS_USER_TOKEN']
        cube_con = ChrisClient(options.CUBEurl, options.CUBEtoken)
        cube_con.health_check()
    except Exception as ex:
        LOG(ex)
        return False
    try:
        # pfdcm health check
        pfdcm.health_check(options.PFDCMurl)
    except Exception as ex:
        LOG(ex)
        return False
    return True

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
def create_query(df: pd.DataFrame):
    l_srch_idx = []
    l_anon_idx = []
    for column in df.columns:
        if "search" in str(column).lower():
            l_srch_idx.append(df.columns.get_loc(column))
        if "anon" in str(column).lower():
            l_anon_idx.append(df.columns.get_loc(column))

    l_job = []

    for row in df.iterrows():
        d_job = {}

        s_col = (df.columns[l_srch_idx].values)
        s_row = (row[1].iloc[l_srch_idx].values)
        s_d = [{k.split('.')[0].split('_')[1]: v} for k, v in zip(s_col, s_row)]
        d_job["search"] = dict(ChainMap(*s_d))

        a_col = (df.columns[l_anon_idx].values)
        a_row = (row[1].iloc[l_anon_idx].values)
        a_d = [{k.split('.')[0].split('_')[1]: v} for k, v in zip(a_col, a_row)]
        d_job["anon"] = dict(ChainMap(*a_d))

        l_job.append(d_job)

    return l_job
