import requests
import xarray as xr
import fsspec
import logging
from datetime import datetime
import math
import copy

from global_vars import GlobalVars
from process_cruises.process_batch_of_cruises import process_batch_of_cruises

session = requests.Session()
a = requests.adapters.HTTPAdapter(max_retries=5)
session.mount('https://', a)


def get_file_id_hash_mapping():

    # Use when checking if file is new and needs to be updated
    # in final json

    query = f"{GlobalVars.API_END_POINT}/file"
    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in function get_file_id_hash_mapping')
        print(response)
        exit(1)

    mapping = response.json()['files']

    # Get into form {file_id: file_hash}
    file_id_hash_mapping = {obj['id']: obj['hash'] for obj in mapping}

    hash_file_id_mapping = {obj['hash']: obj['id'] for obj in mapping}

    return file_id_hash_mapping, hash_file_id_mapping


def setup_test_cruise_objs(netcdf_cruises_objs):

    by_expocode = True
    by_batch = False

    if by_expocode:

        #  BTL & CTD
        # station cast 016_001 has meas = []
        #  "measurementsSource": null,
        # Has ctd temp qc all bad
        # No btl station # 16

        # what's up with limited vars of pres and sample
        # but no other? Seems whole profile should not exist
        # test_cruise_expocode = '096U20160426'

        # This cruise is BTL_CTD
        # uses salinity_btl  for station3 cast 1
        # test_cruise_expocode = '06HF991_1'

        # meas source qc not unique and not None [2.0 None]
        # temp and qc_source are None and 2.0 but has psal val
        # so temp included but what does that mean if it's null?
        # 218_001 station cast
        # test_cruise_expocode = '18HU20130507'

        # test for cacl oxygen_ml_l with qc = 1
        # test_cruise_expocode = '06PO20110601'

        # oxygen_ml_l with qc = 9
        # test_cruise_expocode = '49NZ199909_2'

        # BTL & CTD
        # oxygen_ml_l with qc = 3 for BTL
        # oxygen_ml_l with qc = 2 for CTD
        # Ask using btl salinity for oxy conversion
        # if ctd salinity bad flags
        # ctd_temperature_68 for CTD
        # Can check qc of oxy CTD
        # where sal & temp qc = 9.0
        # Can check qc  of oxy BTL with oxy qc=3
        # and ctd temp no qc so qc =  0
        #test_cruise_expocode = '316N154_2'

        # --------------------

        # CTD with oxygen_ml_l qc = 2.0 and 4.0
        # ctd_salinity has 2.0  and 9.0
        # ctd_temp qc = 2.0 and 9.0
        # see how the qc match up and if
        # different from oxygen qc
        # do I have qc = 2 unless sal and temp not good qc
        # and leave rest of qc alone
        #test_cruise_expocode = '316N145_12'

        # -----------------------

        # Testing key existence ('data_type')
        # BTL and CTD
        # check profile_dict['data_type'] keyerror
        #test_cruise_expocode = '325020210420'

        # -----------------------

        # Bad temperature and salinity
        # All bad so need to not calculate or
        # need a way to catch this error.
        # Looks like no values at all? Weird
        # operands could not be broadcast together
        # with shapes (0,) (6000,) () ()
        # CTD file
        # All temperature values are bad
        # but why not try and calculate?

        # All oxygen values flagged bad = 9
        # Oxy doesn't exist, it's all null
        # Need to check if all values are null
        # Also look to see if I include the oxygen var
        # in data and source keys if all oxy are nan
        # I don't include it. It's removed after
        # processing xarray with conversions

        # Also good case to show measurements = []
        # some have all psal and temp bad, some
        # temp bad and psal good, so measurements = []

        # Has ctd_temperature, but all flagged bad = 1
        # but kept in data set
        #test_cruise_expocode = '49NZ199909_2'

        # ----------------------

        # Case of bad flagged S or T for converting Ox
        # and it failing to go ahead and do it

        # station cast 35 01 doesn't convert units
        # No non nan salinity to convert oxygen units

        # doxy_ctd has no qc
        # other casts do
        # hmmm, looks like not having an oxy qc
        # screwed things up to look like it
        # can't convert because there is ctd sal,
        # it's just flag = 4
        # It's not nan. But maybe don't convert
        # if all flags = 4 for salinity or temp
        # It's the non 0 and 2 flags for all values
        # that causes it not to convert

        # "doxy_ctd": 217.6,
        # "psal_ctd": 32.1522,
        # "psal_ctd_woceqc": 4,

        #test_cruise_expocode = '49KA199905_1'

        # --------------------

        # Says there are 200 included vars
        # but that number doesn't exist
        # test_cruise_expocode = '32MW078_1'

        # BTL and  CTD
        # oxygen and temp conversion
        # More than one O2 var with ml/l
        # for ctd_oxy qc = 3
        # for oxygen, qc = 2 and 9
        # And CTD converts
        #test_cruise_expocode = '316N154_2'

        # oxygen conversion
        # CTD file
        # And has temp:null, psal: #
        # also temp: null, psal: null

        # TODO
        # So has ctd_temperature but no good values
        # Should I skip these profiles and not save them
        #test_cruise_expocode = '49NZ199909_2'

        # 'N_LEVELS': 6070, 'N_PROF': 152
        #  test_cruise_expocode  = '33RO200306_01

        # Many Btl parameters
        # pressure is Geo2D type?
        # test_cruise_expocode = '325020210420

        # test_cruise_expocode = '77DN20010717'

        # meas source qc not unique. [3.0 2.0]
        # here
        test_cruise_expocode = '29HE06_1'

        # Has Oxy in ml/l and bottle salinity
        # but no ctd_salinity
        #test_cruise_expocode = '32OC258_3'

        # This cruise is BTL_CTD
        # here
        #test_cruise_expocode = '64TR90_3'

        # has both btl and ctd and want to filter
        # out meas objs without a temp var
        #test_cruise_expocode = '09AR9601_1'
        # expo_09AR9391_2_sta_063_cast_001.json
        #test_cruise_expocode = '09AR9391_2'

        # Look at btl and ctd profiles that have nan values
        #test_cruise_expocode = '32MW893_3'

        # btl and ctd files
        # has some profiles with btl_ctd meas sources
        # Check how NaN values removed from meas pts
        # measurements_sources
        # {'pres_btl': 'true', 'temp_btl': 'false', 'psal_btl': 'true',
        #     'pres_ctd': 'true', 'temp_ctd': 'true', 'psal_ctd': 'false'}
        #test_cruise_expocode = '33KB184_1'

        # test temp_unk for ctd and temp for btl
        # when combining. Want to exclude ctd in this case
        # test_cruise_expocode = '09AR9601_1'

        # has no ctd vars
        #test_cruise_expocode = '33RO20070710'

        # has btl and ctd
        # test_cruise_expocode = '325020210316'
        # test_cruise_expocode = '325020210420'

        # btl and ctd have diff # of N_PROF
        # test_cruise_expocode = '096U20160426'

        netcdf_cruises_objs = [
            cruise_obj for cruise_obj in netcdf_cruises_objs if cruise_obj['cruise_expocode'] == test_cruise_expocode]

    if by_batch:
        netcdf_cruises_objs = netcdf_cruises_objs[0: 4]

    return netcdf_cruises_objs


def add_file_data(netcdf_cruise_obj):

    cruise_expocode = netcdf_cruise_obj['cruise_expocode']
    file_objs = netcdf_cruise_obj['file_objs']

    file_objs_w_data = []
    for file_obj in file_objs:

        file_path = file_obj['cchdo_file_meta']['file_path']

        try:
            with fsspec.open(file_path) as fobj:
                nc = xr.open_dataset(fobj, engine='h5netcdf',
                                     chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

            # Try this way to see if program
            # doesn't hang while reading in file
            # with fsspec.open(file_path) as fobj:
            #     nc = xr.open_dataset(fobj, engine='h5netcdf')
            #     nc = nc.chunk(chunks={"N_PROF": GlobalVars.CHUNK_SIZE})

        except Exception as e:
            logging.warning(f"Error reading in file {file_path}")
            logging.warning(f"Error {e}")
            logging.info(
                f"Error reading file for cruise {file_obj['cruise_expocode']}")
            logging.info(f"Data type {file_obj['data_type']}")

            return {}

        file_obj['nc'] = nc

        file_objs_w_data.append(file_obj)

    cruise_obj_w_data = {}
    cruise_obj_w_data['cruise_expocode'] = cruise_expocode
    cruise_obj_w_data['file_objs'] = file_objs_w_data

    return cruise_obj_w_data


def get_cruises_data_objs(netcdf_cruises_objs):

    # Add file data into objs

    cruise_objs_w_data = []
    for netcdf_cruise_obj in netcdf_cruises_objs:

        logging.info(
            f"Creating cruise obj {netcdf_cruise_obj['cruise_expocode']}")

        netcdf_cruise_obj_w_data = add_file_data(netcdf_cruise_obj)

        cruise_objs_w_data.append(netcdf_cruise_obj_w_data)

    return cruise_objs_w_data


def get_cchdo_cruise_meta(cruise_json):

    cruise_meta = copy.deepcopy(cruise_json)

    # Remove  internal  CCHDO notes and select meta later to use
    # at end of program

    cruise_meta.pop('notes', None)

    collections = cruise_meta.pop('collections', None)

    if collections:

        if 'groups' in collections.keys():
            groups = collections['groups']
        else:
            groups = []

        if 'oceans' in collections.keys():
            oceans = collections['oceans']
        else:
            oceans = []

        if 'programs' in collections.keys():
            programs = collections['programs']
        else:
            programs = []

        if 'woce_lines' in collections.keys():
            woce_lines = collections['woce_lines']
        else:
            woce_lines = []
    else:
        groups = []
        oceans = []
        programs = []
        woce_lines = []

    participants = cruise_meta.pop('participants', None)

    chief_scientists = []

    if participants:

        for participant in participants:
            name = participant['name']
            institution = participant['institution']
            email = participant['email']
            role = participant['role']

            if role.lower() == "chief scientist":
                chief_scientists.append(name)

    cruise_meta['chief_scientists'] = chief_scientists

    # Reorder programs list so that Go-Ship is first entry
    # since this Argovis program concerned with Go-Ship first
    programs_lowercase = [elem.lower() for elem in programs]

    for index, elem in enumerate(programs_lowercase):

        if elem == 'go-ship':
            programs.pop(index)
            programs.insert(0, 'GO-SHIP')
            break

    # Modify program names
    # Want lowercase and prefixed with 'cchdo_'
    # And if no program, program is cchdo_other
    programs = [f"cchdo_{program}" for program in programs_lowercase]
    if not programs:
        programs = ['cchdo_other']

    cruise_meta['programs'] = programs

    cruise_meta['groups'] = groups
    cruise_meta['oceans'] = oceans
    cruise_meta['woce_lines'] = woce_lines

    expocode = cruise_meta['expocode']

    # Get country code and use ICES code value
    # This is first two numbers in expocode
    country = expocode[0:2]

    cruise_meta['country'] = country

    if '/' in expocode:
        expocode = expocode.replace('/', '_')
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"
    else:
        cruise_url = f"https://cchdo.ucsd.edu/cruise/{expocode}"

    cruise_meta['cruise_url'] = cruise_url

    cruise_id = cruise_meta.pop('id', None)
    cruise_meta['cruise_id'] = cruise_id

    return cruise_meta


def get_cchdo_file_meta(file_json):

    file_meta = copy.deepcopy(file_json)

    # Remove internal CCHDO notes and select file meta
    # to use later at end of program

    file_meta.pop('events', None)
    file_meta.pop('submissions', None)

    # Remove container_contents which is currently not used
    # as of 11/2021
    file_meta.pop('container_contents', None)

    file_path = f"https://cchdo.ucsd.edu{file_meta['file_path']}"
    file_meta['file_path'] = file_path

    return file_meta


def create_file_obj(file_json, cruise_json):

    # Create file object containing file and cruise meta for file
    file_meta = get_cchdo_file_meta(file_json)
    cruise_meta = get_cchdo_cruise_meta(cruise_json)

    file_data_type = file_json['data_type']

    # TODO
    # No need to rename yet
    # Just doing it now because assumed file suffix on data
    # in final json wouldn't change. But now put renaming at
    # end of program
    if file_data_type == 'bottle':
        data_type = 'btl'
    elif file_data_type == 'ctd':
        data_type = 'ctd'
    else:
        # Only looking for bottle and ctd files
        return None

    file_obj = {}
    file_obj['data_type'] = data_type
    file_obj['cchdo_file_meta'] = file_meta
    file_obj['cchdo_cruise_meta'] = cruise_meta

    return file_obj


def check_if_netcdf_data(file_json):

    file_role = file_json['role']
    data_format = file_json['data_format']
    data_type = file_json['data_type']

    is_netcdf_file = file_role == "dataset" and data_format == 'cf_netcdf'
    is_btl = data_type == 'bottle'
    is_ctd = data_type == 'ctd'

    if is_netcdf_file and (is_btl or is_ctd):
        return True
    else:
        return False


def check_if_in_time_range(cruise_json, time_range):

    cruise_start_date = cruise_json['startDate']

    # Some cruises are placeholders and have a blank start date
    if not cruise_start_date:
        return False

    cruise_datetime = datetime.strptime(cruise_start_date, "%Y-%m-%d")

    in_date_range = cruise_datetime >= time_range['start'] and cruise_datetime <= time_range['end']

    return in_date_range


def get_active_files_json():

    # Get JSON metadata of all CCHDO active files

    # At CCHDO, all files are kept in the database, and are
    # designated as active for use in cruises
    # They may or may not be attached to a cruise. To find this out
    # need to query cruise json which lists files attached to cruise

    # Use api query to get all active file ids from CCHDO
    query = f"{GlobalVars.API_END_POINT}/file/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_files')
        print(response)
        exit(1)

    return response.json()


def get_active_cruises_json():

    # Get all active cruises JSON meta

    # CCHDO stores all cruises created, even deleted ones,
    # so only want to retrieve active ones

    # Use api query to get all cruise id with their attached file ids
    query = f"{GlobalVars.API_END_POINT}/cruise/all"

    response = session.get(query)

    if response.status_code != 200:
        print('api not reached in get_all_cruises')
        print(response)
        exit(1)

    cruises_json = response.json()

    # Remove any with no start date
    cruises_json = [cruise for cruise in cruises_json if cruise['startDate']]

    def get_date(cruise_start_date):
        return datetime.strptime(cruise_start_date, "%Y-%m-%d")

    cruises_json.sort(
        key=lambda item: get_date(item['startDate']), reverse=True)

    return cruises_json


def process_all_cruises(time_range):

    # Get active cruises and active NetCDF CF files
    # attached to them. Will use json
    # information as metadata
    active_cruises_json = get_active_cruises_json()
    active_files_json = get_active_files_json()

    # Get file ids of NetCDF CF files
    netcdf_file_id_json_mapping = {}
    netcdf_file_ids = []

    for file_json in active_files_json:

        is_netcdf = check_if_netcdf_data(file_json)

        if is_netcdf:

            file_id = file_json['id']

            netcdf_file_id_json_mapping[file_id] = file_json
            netcdf_file_ids.append(file_id)

    # Now find cruises with the NetCDF CF active files attached
    netcdf_cruises_objs = []
    for cruise_json in active_cruises_json:

        in_time_range = check_if_in_time_range(cruise_json, time_range)

        if not in_time_range:
            continue

        # Get files attached to the cruise which
        # could be inactive ones so check if file exists in all_files
        # which is a list of active files
        cruise_file_ids = cruise_json['files']

        cruise_netcdf_file_ids = [
            id for id in cruise_file_ids if id in netcdf_file_ids]

        if not cruise_netcdf_file_ids:
            continue

        # cruise_netcdf_file_ids = [id for id in cruise_netcdf_file_ids if id]

        # create file objects which will contain file and cruise meta
        # for each file
        file_objs = []
        for file_id in cruise_netcdf_file_ids:

            # Get the Netcdf file json which was stored earlier
            file_json = netcdf_file_id_json_mapping[file_id]

            file_obj = create_file_obj(file_json, cruise_json)

            file_objs.append(file_obj)

        cruise_obj = {}
        if file_objs:
            cruise_obj['cruise_expocode'] = cruise_json['expocode']
            cruise_obj['file_objs'] = file_objs

        if cruise_obj:
            netcdf_cruises_objs.append(cruise_obj)

    # TODO
    # Create better way of setting up test objects and running tests
    if GlobalVars.TEST:
        netcdf_cruises_objs = setup_test_cruise_objs(netcdf_cruises_objs)

    num_netcdf_cruises_objs = len(netcdf_cruises_objs)
    num_in_batch = GlobalVars.NUM_IN_BATCH

    num_batches = math.floor(num_netcdf_cruises_objs/num_in_batch)
    num_leftover = num_netcdf_cruises_objs % num_in_batch

    logging.info(f"Total cruises {num_netcdf_cruises_objs}")
    logging.info(f"num batches {num_batches} and num leftover {num_leftover}")

    for start in range(0, num_batches):

        start_batch = start * num_in_batch
        end_batch = start_batch + num_in_batch

        logging.info(f"start batch {start_batch}")
        logging.info(f"end batch {end_batch}")

        netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: end_batch]

        # Add data to the objs
        cruises_data_objs_w_data = get_cruises_data_objs(
            netcdf_cruises_objs_batch)

        process_batch_of_cruises(cruises_data_objs_w_data)

    if num_leftover:

        logging.info("Inside cruise objs leftover loop")

        start_batch = num_batches * num_in_batch

        netcdf_cruises_objs_batch = netcdf_cruises_objs[start_batch: num_netcdf_cruises_objs]

        # Add data to the objs
        cruises_data_objs_w_data = get_cruises_data_objs(
            netcdf_cruises_objs_batch)

        process_batch_of_cruises(cruises_data_objs_w_data)
