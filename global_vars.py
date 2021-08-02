class GlobalVars:

    TEST = False

    API_END_POINT = "https://cchdo.ucsd.edu/api/v1"

    LOGGING_DIR = './logging'

    JSON_DIR = './converted_data'

    INCLUDE_EXCLUDE_DIR = './logging/included_excluded'

    OUTPUT_LOG = 'output_log'

    # When chunk, make sure less than chunk is also computed
    # Just look at output when N_PROF < 30
    CHUNK_SIZE = 30

    NUM_IN_BATCH = 200
