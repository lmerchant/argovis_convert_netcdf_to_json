import requests

API_END_POINT = "https://cchdo.ucsd.edu/api/v1"


def get_expocode_file_id():

    # expocode =  06GA350_1, cruise_id = 865
    # ctd file id = 17346
    # expocode = '06GA350_1'
    # file_id = 17346

    # # ctd file
    # file_id = 18420
    # expocode = '316N154_2'

    # btl file
    # many params
    # no temp_qc
    # expocode = '325020210420'
    # file_id = 19429

    # ctd file
    # doxy in ctd_oxygen_ml_l
    # doxy not given a suffix
    # expocode = '06PO20110601'
    # file_id = 18620

    # ctd file
    # has both ctd temp and ctd temp 68
    # And has case where no temp in measurements (all NaN because no qc=0 or 2)
    # Case is where using diff scale of ctd temp, so one is null while 68 is not
    # Look at station: 021 and cast: 001
    # expocode = '49K6KY9606_1'
    # file_id = 17365

    #  ctd file
    # expocode = '325020210420'
    # file_id = 19427

    # btl file
    # expocode = '325020210420'
    # file_id = 19429

    # hangs, ctd
    # expocode = '33RR20160321'
    # file_id = 17904

    # ctd temp unknown
    # expocode = '33RO20070710
    # file_id = 17772

    # ctd file (very large)
    # has ctd temp 68 and ctd temp unk
    # also has both oxygen units
    # expocode = '90VE43_1'
    # file_id = 17879

    # btl file
    # expocode = '33KI20180723'
    # file_id = 19095

    # error  for ctd temp unk
    # expocode = '33RO20070710'
    # file_id = 17772

    # CTD file error on meta chunk dim
    # since some are constants
    #  Also  has  ctd_temperature_68
    # and oxygen_ml_l
    # to check conversion
    # expocode = '316N154_2'
    # file_id = 18420

    # temp_ctd renaming conflict
    # ctd_temp and ctd_temp_68
    # expocode = '49K6KY9606_1'
    # file_id = 17365

    # # doxy_ctd renaming conflict
    # expocode = '33SW9404_1'
    # file_id = 18400

    # no ctd_temp to calc o2 conversion
    # It has ctd_temperature_unk and oxygen_ml_l
    # expocode = '06MT30_3'
    # file_id = 17773

    # Check salinity c_format for BIOS20140819
    # The values look to be only 3 decimal places
    # And the c_format is 4 decimal places
    # does this mean precision is 3 but when do math,
    # assuming it is good out to 4 places
    # I wonder if this is true for all BIOS cruises
    # No cruise report
    # expocode = 'BIOS20140819'
    # file_id = 18353

    # Cruise with no ctd_salinity
    # btl file
    expocode = '77DN20010717'
    btl_file_id = 17139
    ctd_file_id = None

    # Every time start over,
    # included/excluded logs wiped out
    # expocode = '77DN20010717'
    # file_id = 17139

    return expocode, btl_file_id, ctd_file_id


def create_test_obj(expocode, file_id, data_type):

    query = f"{API_END_POINT}/file/{file_id}"

    response = requests.get(query)

    if response.status_code != 200:
        print('api not reached in function setup_test_objs')
        print(response)
        exit(1)

    file_json = response.json()

    file_path = f"https://cchdo.ucsd.edu{file_json['file_path']}"

    test_obj = {}
    test_obj['data_type'] = data_type
    test_obj['cruise_expocode'] = expocode
    test_obj['cruise_id'] = 9999
    test_obj['woce_lines'] = 'woce_lines'
    test_obj['file_path'] = file_path
    test_obj['filename'] = 'filename'
    test_obj['file_hash'] = 'file_hash'

    return test_obj


def setup_test_objs():

    test_objs = []

    expocode, btl_file_id, ctd_file_id = get_expocode_file_id()

    if btl_file_id:
        btl_obj = create_test_obj(expocode, btl_file_id, 'btl')
        test_objs.append(btl_obj)
    elif ctd_file_id:
        ctd_obj = create_test_obj(expocode, ctd_file_id, 'ctd')
        test_objs.append(ctd_obj)

    return test_objs
