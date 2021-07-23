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

    collections = {}
    collections['included'] = []
    collections['excluded'] = []

    if GlobalVars.TEST:
        test_objs = setup_test_objs()
        process_files(test_objs, collections)
        exit(1)

    cruises_json, files_info = get_all_cruises_file_info()

    cruise_count = 0

    for cruise_json in cruises_json:
        cruise_count = process_cruise(cruise_json, files_info, time_range,
                                      collections, cruise_count)

    logging.info(f"Total number of cruises converted {cruise_count}")
    logging.info('=======================================')

    logging.info("Time to run program")
    logging.info(datetime.now() - program_start_time)


if __name__ == '__main__':

    main()
