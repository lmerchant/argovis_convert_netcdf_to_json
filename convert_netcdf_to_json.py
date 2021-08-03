import ctypes
from datetime import date, datetime
import os
import shutil
import logging
import click
from dask.diagnostics import ProgressBar
from dask.distributed import Client
from dask.diagnostics import Profiler

from global_vars import GlobalVars
from setup_logging import setup_logging
from process_cruises.get_cruises_file_info import get_all_cruises_file_info
from process_cruises.process_all_cruises import process_all_cruises


pbar = ProgressBar()
pbar.register()


@ click.command()
@ click.option('-s', '--start_year', default=1900, help='Start year')
@ click.option('-e', '--end_year', default=date.today().year, help='End year')
@ click.option('-a', '--append', is_flag=True, help='Append logs')
def main(start_year, end_year, append):

    program_start_time = datetime.now()

    setup_logging(append)

    logging.info(f"Converting years Jan 1, {start_year} to Dec 31, {end_year}")

    time_range = {}
    time_range['start'] = datetime(start_year, 1, 1)
    time_range['end'] = datetime(end_year, 12, 31)

    cruises_json, files_info = get_all_cruises_file_info()

    data_dir = GlobalVars.JSON_DIR
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    os.makedirs(data_dir, exist_ok=True)

    process_all_cruises(cruises_json, files_info, time_range)

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':

    # To troubleshoot, set dask config to ?
    # So it is not parallel
    # dask.config.set(scheduler='single-threaded')
    # client()

    # does this change dask config yaml file?
    # if so, need to use refresh option to update  it
    # if change anything

    # client = Client(memory_limit='4GB', processes=True,
    #                 n_workers=4, dashboard_address=None)

    # client

    # def trim_memory() -> int:
    #     libc = ctypes.CDLL("libc.so.6")
    #     return libc.malloc_trim(0)

    # client.run(trim_memory)

    main()

    # client.close()
