from datetime import date, datetime
import os
import logging
import click
from dask.diagnostics import ProgressBar

from global_vars import GlobalVars
from setup_logging import setup_logging
from get_cruises_file_info import get_all_cruises_file_info
from process_cruise import process_cruise
from tests.setup_test_objs import setup_test_objs
from process_files import process_files
from process_cruises_dask import process_cruises_dask
from check_and_save.save_output import save_included_excluded_goship_vars_dask


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

    os.makedirs(GlobalVars.JSON_DIR, exist_ok=True)

    if GlobalVars.TEST:
        test_objs = setup_test_objs()
        process_files(test_objs)
        exit(1)

    cruises_json, files_info = get_all_cruises_file_info()

    # TODO
    # Is it faster to read in all the files and then
    # process using Dask?

    #process_cruises_dask(cruises_json, files_info, time_range)

    cruise_count = 0

    all_included = []
    all_excluded = []

    for cruise_json in cruises_json:

        cruise_count, included,  excluded = process_cruise(
            cruise_json, files_info, time_range, cruise_count)

        all_included.extend(included)
        all_excluded.extend(excluded)

    # ***********************************
    # Write included/excluded goship vars
    # ***********************************

    logging.info("Save included and excluded goship vars")

    save_included_excluded_goship_vars_dask(included, excluded)

    logging.info(f"Total number of cruises converted {cruise_count}")
    logging.info('=======================================')

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':

    main()
