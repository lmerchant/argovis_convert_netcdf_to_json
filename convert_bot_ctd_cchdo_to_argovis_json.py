import requests
import fsspec
import xarray as xr
import dask.array as da
import dask.dataframe as dd
from datetime import datetime
from datetime import date
import errno
import os
import logging
import click
import dask as ds
from dask.distributed import Client
from dask.diagnostics import ProgressBar
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
from dask.diagnostics import visualize
from cachey import nbytes
import ctypes


import check_if_has_ctd_vars as ckvar
import filter_measurements as fm
import get_cruise_information as gi
import create_profiles_one_type as op
import create_profiles_combined_type as cbp
import save_output as sv


# Dask bag default is multiprocessing


pbar = ProgressBar()
pbar.register()

# fs = fsspec.filesystem("filecache", target_protocol='fs',
#                        cache_storage='/tmp/fsspec_files/')


# install cloudpickle to use the multiprocessing scheduler for dask

# In order to use xarray open_dataset

# https://github.com/pydata/xarray/issues/3653
# pip install aiohttp
# pip install h5netcdf

# url = 'https://cchdo.ucsd.edu/data/16923/318M20130321_bottle.nc'
# with fsspec.open(url) as fobj:
#     ds = xr.open_dataset(fobj)

# For testing with a single thread
# c.compute(scheduler='single-threaded')

# Dask needs bokeh >= 0.13.0 for the dashboard.
# Dashboard at http://localhost:8787/status
# https://docs.dask.org/en/latest/setup/single-distributed.html


"""

Convert CCHDO CTD and bottle CF netCDF files to ArgoVis JSON format

program by: Lynne Merchant
date: 2021

"""


def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


def dtjson(o):
    if isinstance(o, datetime):
        return o.isoformat()


def remove_file(filename, dir):
    filepath = os.path.join(dir, filename)

    try:
        os.remove(filepath)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


def read_file_test(data_obj):

    data_path = data_obj['data_path']

    nc = xr.open_dataset(data_path)

    data_obj['nc'] = nc

    file_expocode = nc.coords['expocode'].data[0]

    meta_names, param_names = pm.get_meta_param_names(nc)

    data_obj['meta'] = meta_names
    data_obj['param'] = param_names

    data_obj['file_expocode'] = file_expocode

    return data_obj


def read_file(data_obj):

    err_flag = ''
    chunk_size = 20

    data_path = data_obj['data_path']

    data_url = f"https://cchdo.ucsd.edu{data_path}"

    try:
        # with fsspec.open(data_url) as fobj:
        #     nc = xr.open_dataset(fobj, engine='h5netcdf',
        #                          chunks={"N_PROF": chunk_size})

        with fsspec.open(data_url) as fobj:
            nc = xr.open_dataset(fobj, engine='h5netcdf')
            nc = nc.chunk(chunks={"N_PROF": chunk_size})

    except Exception as e:
        logging.warning(f"Error reading in file {data_url}")
        logging.warning(f"Error {e}")
        err_flag = 'error'
        return data_obj, err_flag

    data_obj['nc'] = nc
    data_obj['chunk_size'] = chunk_size

    return data_obj, err_flag


def setup_test_obj(dir, filename, type):

    if type == 'btl':
        btl_obj = {}
        btl_obj['found'] = True
        btl_obj['type'] = 'btl'
        btl_obj['data_path'] = os.path.join(dir, filename)
        btl_obj['filename'] = filename

        return btl_obj

    if type == 'ctd':
        ctd_obj = {}
        ctd_obj['found'] = True
        ctd_obj['type'] = 'ctd'
        ctd_obj['data_path'] = os.path.join(dir, filename)
        ctd_obj['filename'] = filename

        return ctd_obj


def setup_testing(btl_file, ctd_file, test_btl, test_ctd):

    # TODO
    # Make sure included all btl and ctd keys

    input_dir = './testing_data/modify_data_for_testing'
    output_dir = './testing_output'
    os.makedirs(output_dir, exist_ok=True)

    btl_obj = {}
    ctd_obj = {}
    btl_obj['found'] = False
    ctd_obj['found'] = False

    cruise_info = {}
    cruise_info['btl'] = {}
    cruise_info['ctd'] = {}
    cruise_info['btl']['found'] = False
    cruise_info['ctd']['found'] = False
    cruise_info['expocode'] = 'testing'
    cruise_info['cruise_id'] = None

    all_cruises_info = []
    all_cruises_info.append(cruise_info)

    # Enter test files
    if test_btl:
        btl_obj = setup_test_obj(input_dir, btl_file, 'btl')

        logging.info("======================\n")
        logging.info(f"Processing btl test file {btl_file}")

    if test_ctd:
        ctd_obj = setup_test_obj(input_dir, ctd_file, 'ctd')

        logging.info("======================\n")
        logging.info(f"Processing ctd test file {ctd_file}")

    return output_dir, all_cruises_info, btl_obj, ctd_obj


def setup_logging(append_logs):

    logging_dir = './logging'
    os.makedirs(logging_dir, exist_ok=True)

    included_excluded_dir = './logging/included_excluded'
    os.makedirs(included_excluded_dir, exist_ok=True)

    if not append_logs:
        remove_file('output.log', logging_dir)
        remove_file('file_read_errors.txt', logging_dir)
        remove_file('found_goship_units.txt', logging_dir)
        remove_file('cruises_no_core_ctd_vars.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_qc.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_ref_scale.txt', logging_dir)
        remove_file('cruises_no_ctd_temp.txt', logging_dir)
        remove_file('cruises_no_expocode.txt', logging_dir)
        remove_file('cruises_no_pressure.txt', logging_dir)
        remove_file('found_cruises_with_coords_netcdf.txt', logging_dir)
        remove_file('diff_cruise_and_file_expocodes.txt', logging_dir)
        remove_file('cruises_not_converted.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_unk.txt', logging_dir)

    filename = 'output.log'
    logging_path = os.path.join(logging_dir, filename)
    logging.root.handlers = []
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                        level=logging.INFO, filename=logging_path)

    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    # return logging_dir, logging
    return logging_dir, included_excluded_dir


@ click.command()
@ click.option('-s', '--start_year', default=1950, help='Start year')
@ click.option('-e', '--end_year', default=date.today().year, help='End year')
@ click.option('-a', '--append', is_flag=True, help='Append logs')
def main(start_year, end_year, append):

    program_start_time = datetime.now()

    # logging_dir, logging = setup_logging(append)
    logging_dir, included_excluded_dir = setup_logging(append)

    logging.info(f"Converting years Jan 1, {start_year} to Dec 31, {end_year}")

    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31)

    # First create a list of the cruises found
    # with netCDF files

    session = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    session.mount('https://', a)

    json_directory = './converted_data'
    os.makedirs(json_directory, exist_ok=True)

    # TESTING
    testing = False
    test_btl_obj = {}
    test_ctd_obj = {}

    if testing:

        # Change this to do bot and ctd separate or combined
        test_btl = False
        test_ctd = True

        btl_file = 'modified_318M20130321_bottle_no_psal.nc'
        ctd_file = 'modified_318M20130321_ctd_core_bad_flag.nc'

        json_directory, all_cruises_info, test_btl_obj, test_ctd_obj = setup_testing(
            btl_file, ctd_file, test_btl, test_ctd)

        btl_found = test_btl_obj['found']
        ctd_found = test_ctd_obj['found']

    else:
        # Loop through all cruises and grap NetCDF files
        all_cruises_info = gi.get_cruise_information(
            session, logging_dir, start_datetime, end_datetime)

        # cruise_info = gi.get_information_one_cruise_test(session)
        # all_cruises_info = [cruise_info]

        if not all_cruises_info:
            logging.info('No cruises within dates selected')
            exit(1)

    read_error_count = 0
    data_file_read_errors = []

    goship_collection = []
    excluded_collection = []

    for cruise_info in all_cruises_info:

        if not testing:

            cruise_expocode = cruise_info['expocode']

            logging.info("--------------------------------")
            logging.info(f"Start converting Cruise: {cruise_expocode}")
            logging.info("--------------------------------")

            btl_found = cruise_info['btl']['found']
            ctd_found = cruise_info['ctd']['found']

            cruise_info['btl']['cruise_expocode'] = cruise_expocode
            cruise_info['ctd']['cruise_expocode'] = cruise_expocode

        if btl_found:

            if testing:
                btl_obj = read_file_test(test_btl_obj)

            else:
                btl_obj = cruise_info['btl']

                btl_obj, flag = read_file(btl_obj)

                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{btl_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

                profiles_btl = op.create_profiles_one_type(
                    btl_obj, logging_dir)

        if ctd_found:

            if testing:
                ctd_obj = read_file_test(test_ctd_obj)

            else:
                ctd_obj = cruise_info['ctd']

                ctd_obj, flag = read_file(ctd_obj)

                if flag == 'error':
                    print("Error reading file")
                    read_error_count = read_error_count + 1
                    data_url = f"https://cchdo.ucsd.edu{ctd_obj['data_path']}"
                    data_file_read_errors.append(data_url)
                    continue

                profiles_ctd = op.create_profiles_one_type(
                    ctd_obj, logging_dir)

        if btl_found and ctd_found:

            # filter measurements when combine btl and ctd profiles
            # don't filter btl or ctd first in case need
            # a variable from both before they are filtered out
            profiles_btl_ctd = cbp.combine_btl_ctd_profiles(
                profiles_btl, profiles_ctd)

        # Now check if profiles have CTD vars and should be saved
        # And filter btl and ctd measurements separately

        if btl_found and ctd_found:

            checked_ctd_variables, ctd_vars_flag = ckvar.check_of_ctd_variables(
                profiles_btl_ctd, logging_dir)

            if ctd_vars_flag:
                logging.info('----------------------')
                logging.info('Saving files')
                logging.info('----------------------')
                sv.write_profile_goship_units(
                    checked_ctd_variables, logging_dir)
                sv.save_all_btl_ctd_profiles(checked_ctd_variables, goship_collection, excluded_collection,
                                             json_directory)

            else:
                logging.info(
                    f"*** Cruise not converted {cruise_expocode}")
                filename = 'cruises_not_converted.txt'
                filepath = os.path.join(logging_dir, filename)
                with open(filepath, 'a') as f:
                    f.write(f"{cruise_expocode}\n")

        elif btl_found:
            # filter measurements for core
            profiles_btl = fm.filter_measurements(profiles_btl, 'btl')

            checked_ctd_variables, ctd_vars_flag = ckvar.check_of_ctd_variables(
                profiles_btl, logging_dir)

            if ctd_vars_flag:
                logging.info('----------------------')
                logging.info('Saving files')
                logging.info('----------------------')
                sv.write_profile_goship_units(
                    checked_ctd_variables, logging_dir)
                sv.save_all_profiles_one_type(
                    checked_ctd_variables, goship_collection, excluded_collection, json_directory)

            else:
                logging.info(
                    f"*** Cruise not converted {cruise_expocode}")
                filename = 'cruises_not_converted.txt'
                filepath = os.path.join(logging_dir, filename)
                with open(filepath, 'a') as f:
                    f.write(f"{cruise_expocode}\n")

        elif ctd_found:
            # filter measurements for core
            profiles_ctd = fm.filter_measurements(profiles_ctd, 'ctd')

            checked_ctd_variables, ctd_vars_flag = ckvar.check_of_ctd_variables(
                profiles_ctd, logging_dir)

            if ctd_vars_flag:
                logging.info('----------------------')
                logging.info('Saving files')
                logging.info('----------------------')
                sv.write_profile_goship_units(
                    checked_ctd_variables, logging_dir)

                sv.save_all_profiles_one_type(
                    checked_ctd_variables, goship_collection, excluded_collection, json_directory)

            else:
                logging.info(
                    f"*** Cruise not converted {cruise_expocode}")
                filename = 'cruises_not_converted.txt'
                filepath = os.path.join(logging_dir, filename)
                with open(filepath, 'a') as f:
                    f.write(f"{cruise_expocode}\n")

        if btl_found or ctd_found:

            if btl_found:
                cruise_expocode = btl_obj['cruise_expocode']

            if ctd_found:
                cruise_expocode = ctd_obj['cruise_expocode']

            logging.info('---------------------------')
            logging.info(
                f"Finished for cruise {cruise_expocode}")
            logging.info('---------------------------')

            logging.info("*****************************\n")

    # ***********************************
    # Write included/excluded goship vars
    # ***********************************

    logging.info("Save included and excluded goship vars")

    sv.save_included_excluded_goship_vars(
        included_excluded_dir, goship_collection, excluded_collection)

    if read_error_count:
        filename = 'file_read_errors.txt'
        logging.warning(f"Errors reading in {read_error_count} files")
        logging.warning(f"See {filename} for files")

        filepath = os.path.join(logging_dir, filename)
        with open(filepath, 'w') as f:
            for line in data_file_read_errors:
                f.write(f"{line}\n")

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':

    # By default Client() forks processes. If you use it within a module you should hide it within the __main__ block.

    # setting Client() without arguments
    # This sets up a scheduler in your local process along with a number of workers and threads per worker related to the number of cores in your machine.
    # client = Client(dashboard_address=None)
    # client = Client()
    # client.get_worker_logs()

    # client = Client(memory_limit='2GB', processes=False,
    #  n_workers = 2, threads_per_worker = 2)

    # client = Client(n_workers=4,
    #                 threads_per_worker=1,
    #                 memory_limit='4GB')

    # https://docs.dask.org/en/latest/setup/single-machine.html
    # ds.config.set(scheduler='threads')
    # ds.config.set(scheduler='single-threaded')

    # https://docs.dask.org/en/latest/diagnostics-local.html
    # prof = Profiler()
    # rprof = ResourceProfiler(dt=0.5)
    # cprof = CacheProfiler(metric=nbytes)

    # visualize([prof, rprof, cprof])

    # client.write_scheduler_file('schedular_output.txt')

    # https://github.com/dask/dask-jobqueue/issues/391
    # Set dashboard to none to prevent program freezing

    # For debugging, use single-threaded
    # client = Client(scheduler='single-threaded')

    # client = Client(processes=False, dashboard_address=None)

    # https://www.javaer101.com/en/article/18616059.html
    # However, if you are spending most of your compute time manipulating Pure Python objects like strings or dictionaries then you may want to avoid GIL issues by having more processes with fewer threads each

    # client = Client(n_workers=4, threads_per_worker=1, dashboard_address=None)

    #  --------------------

    # client = Client(n_workers=4, threads_per_worker=1)
    # client = Client('127.0.0.1:8787')
    # client = Client(processes=False)

    # client.run(trim_memory)

    # https://stackoverflow.com/questions/51212688/how-to-use-all-the-cpu-cores-using-dask/51245571

    # See http: // dask.pydata.org/en/latest/scheduler-overview.html

    # It is likely that the functions that you are calling are pure-python, and so claim the GIL, the lock which ensures that only one python instruction is being carried out at a time within a thread. In this case, you will need to run your functions in separate processes to see any parallelism. You could do this by using the multiprocess scheduler

    # ser = ser.apply(fun1).apply(fun2).compute(scheduler='processes')

    # or by using the distributed scheduler(which works fine on a single machine, and actually comes with some next-generation benefits, such as the status dashboard)
    # in the simplest, default case, creating a client is enough:

    # By default a single Worker runs many computations in parallel using as many threads as your compute node has cores. When using pure Python functions this may not be optimal and you may instead want to run several separate worker processes on each node, each using one thread. When configuring your cluster you may want to use the options to the dask-worker executable as follows:
    # dask-worker ip:port --nprocs 8 --nthreads 1

    # https://www.javaer101.com/en/article/18616059.html
    # However, if you are spending most of your compute time manipulating Pure Python objects like strings or dictionaries then you may want to avoid GIL issues by having more processes with fewer threads each

    # dask-worker ... --nprocs 8 - -nthreads 1

    # Using more processes avoids GIL issues, but adds costs due to inter-process communication. You would want to avoid many processes if your computations require a lot of inter-worker communication..

    main()
