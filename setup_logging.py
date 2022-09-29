import os
import logging
import errno
from pathlib import Path


from global_vars import GlobalVars


def setup_logger():

    filename = GlobalVars.OUTPUT_LOG
    logging_path = os.path.join(GlobalVars.LOGGING_DIR, filename)
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


def remove_file(filename, dir):
    filepath = os.path.join(dir, filename)

    try:
        os.remove(filepath)
    except OSError as e:
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


def delete_logs(append_logs):

    logging_dir = GlobalVars.LOGGING_DIR

    if not append_logs:
        remove_file(GlobalVars.OUTPUT_LOG, logging_dir)
        remove_file('file_read_errors.txt', logging_dir)
        remove_file('found_cchdo_units.txt', logging_dir)
        remove_file('cruises_no_core_ctd_vars.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_qc.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_no_ref_scale.txt', logging_dir)
        remove_file('cruise_files_no_ctd_temp.txt', logging_dir)
        remove_file('cruises_no_expocode.txt', logging_dir)
        remove_file('cruise_files_no_pressure.txt', logging_dir)
        remove_file('found_cruises_with_coords_netcdf.txt', logging_dir)
        remove_file('diff_cruise_and_file_expocodes.txt', logging_dir)
        remove_file('cruise_files_not_converted.txt', logging_dir)
        remove_file('cruises_w_ctd_temp_unk.txt', logging_dir)
        remove_file('profiles_information.txt', logging_dir)
        remove_file('all_cruises_not_processed.txt', logging_dir)
        remove_file('all_cruises_processed.txt', logging_dir)

    include_exclude_dir = GlobalVars.INCLUDE_EXCLUDE_DIR

    [f.unlink() for f in Path(include_exclude_dir).glob("*") if f.is_file()]


def setup_logging(append_logs):

    os.makedirs(GlobalVars.LOGGING_DIR, exist_ok=True)
    os.makedirs(GlobalVars.INCLUDE_EXCLUDE_DIR, exist_ok=True)

    delete_logs(append_logs)

    setup_logger()
