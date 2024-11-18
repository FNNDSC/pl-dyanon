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

__version__ = '1.0.0'

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


parser = ArgumentParser(description='!!!CHANGE ME!!! An example ChRIS plugin which '
                                    'counts the number of occurrences of a given '
                                    'word in text files.',
                        formatter_class=ArgumentDefaultsHelpFormatter)

parser.add_argument('-V', '--version', action='version',
                    version=f'%(prog)s {__version__}')
parser.add_argument(
    "--pattern",
    default="**/*csv",
    help="""
            pattern for file names to include (you should quote this!)
            (this flag triggers the PathMapper on the inputdir).""",
)
parser.add_argument(
    "--pluginInstanceID",
    default="",
    help="plugin instance ID from which to start analysis",
)
parser.add_argument(
    "--searchIdx",
    default="",
    help="comma separated indices of columns containing search data",
)
parser.add_argument(
    "--anonIdx",
    default="",
    help="comma separated indices of columns containing anonymization data",
)
parser.add_argument(
    "--CUBEurl", default="http://localhost:8000/api/v1/", help="CUBE URL"
)
parser.add_argument("--CUBEuser", default="chris", help="CUBE/ChRIS username")
parser.add_argument("--CUBEpassword", default="chris1234", help="CUBE/ChRIS password")
parser.add_argument('--orthancUrl', '-o',
                  dest='orthancUrl',
                  type=str,
                  help='Orthanc server url',
                  default='http://0.0.0.0:8042')

parser.add_argument('--orthancUsername', '-u',
                  dest='username',
                  type=str,
                  help='Orthanc server username',
                  default='orthanc')

parser.add_argument('--orthancPassword', '-p',
                  dest='password',
                  type=str,
                  help='Orthanc server password',
                  default='orthanc')

parser.add_argument('--pushToRemote', '-r',
                  dest='pushToRemote',
                  type=str,
                  help='Remote modality',
                  default='')
parser.add_argument('--PACSurl', default='', type=str,
                    help='endpoint URL of pfdcm')
parser.add_argument('--PACSname', default='MINICHRISORTHANC', type=str,
                    help='name of the PACS')
parser.add_argument(
    "--pftelDB",
    help="an optional pftel telemetry logger, of form '<pftelURL>/api/v1/<object>/<collection>/<event>'",
    default="",
)


# The main function of this *ChRIS* plugin is denoted by this ``@chris_plugin`` "decorator."
# Some metadata about the plugin is specified here. There is more metadata specified in setup.py.
#
# documentation: https://fnndsc.github.io/chris_plugin/chris_plugin.html#chris_plugin
@chris_plugin(
    parser=parser,
    title='A dynamic anonymization ChRIS plugin',
    category='',                 # ref. https://chrisstore.co/plugins
    min_memory_limit='100Mi',    # supported units: Mi, Gi
    min_cpu_limit='1000m',       # millicores, e.g. "1000m" = 1 CPU core
    min_gpu_limit=0              # set min_gpu_limit=1 to enable GPU
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

    # Typically it's easier to think of programs as operating on individual files
    # rather than directories. The helper functions provided by a ``PathMapper``
    # object make it easy to discover input files and write to output files inside
    # the given paths.
    #
    # Refer to the documentation for more options, examples, and advanced uses e.g.
    # adding a progress bar and parallelism.
    mapper = PathMapper.file_mapper(inputdir, outputdir, glob=options.pattern)
    for input_file, output_file in mapper:

        df = pd.read_csv(input_file,dtype=str)
        l_job = create_query(df, options.searchIdx, options.anonIdx)

        for d_job in l_job:
            d_job["send"] = {
                "url" : options.orthancUrl,
                "username" : options.username,
                "password" : options.password,
                "aec"      : options.pushToRemote

            }
            LOG(d_job)

            search_dir, _ = pfdcm.sanitize(d_job["search"])

            # search for DICOMs in PACS
            search_response = pfdcm.get_pfdcm_status(search_dir, options.PACSurl, options.PACSname)
            autofill_directive, count = pfdcm.autocomplete_directive(d_job["search"], search_response)
            LOG(f"{count} files found matching in PACS")
            if count > 0:

                # register DICOMs using pfdcm
                response = pfdcm.register_pacsfiles(autofill_directive, options.PACSurl, options.PACSname)
                d_response = json.loads(response.text)
                LOG(d_response)

                # status for DICOMs in PACS
                search_response = pfdcm.get_pfdcm_status(autofill_directive, options.PACSurl, options.PACSname)
                d_s_resp = json.loads(search_response.text)
                LOG(d_s_resp)

                # create connection object
                cube_con = ChrisClient(options.CUBEurl,options.CUBEuser, options.CUBEpassword)

                # verify registration
                series = cube_con.cl.get_pacs_series_list(autofill_directive)
                while not series['total'] > 0:
                    LOG("waiting for registration")
                    time.sleep(2)
                    series = cube_con.cl.get_pacs_series_list(autofill_directive)

                d_job["search"] = autofill_directive
                cube_con.anonymize(d_job, options.pluginInstanceID)



if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
def create_query(df: pd.DataFrame, str_srch_idx: str, str_anon_idx: str):
    l_srch_idx = list(map(int,str_srch_idx.split(',')))
    l_anon_idx = list(map(int,str_anon_idx.split(',')))

    l_job = []

    for row in df.iterrows():
        d_job = {}

        s_col = (df.columns[l_srch_idx].values)
        s_row = (row[1].iloc[l_srch_idx].values)
        s_d = [{k: v} for k, v in zip(s_col, s_row)]
        d_job["search"] = dict(ChainMap(*s_d))

        a_col=(df.columns[l_anon_idx].values)
        a_row=(row[1].iloc[l_anon_idx].values)
        a_d = [{k.split('.')[0]:v} for k,v in zip(a_col,a_row)]
        d_job["anon"] = dict(ChainMap(*a_d))

        l_job.append(d_job)

    return l_job



