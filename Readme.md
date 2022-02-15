# Convert Bottle and CTD files US Go-Ship cruises to ArgoVis JSON

## Overview

Convert US Go-Ship CF netCDF Bottle and CTD files from CCHDO into ArgoVis JSON
formatted files.

## Conversion Steps

### Read in files

Reads in Bottle and CTD CF netCDF files from cchdo.ucsd.edu for US Go-Ship
cruises. Uses get requests to the CCHDO API cchdo.ucsd.edu/api/v1 to get the
cruise information and path to the data files.

### Convert to ArgoVis Reference Scale

Converts CTD Temperature to the ITS-90 scale if it's on the IPTS-68 scale. At
the moment, only CTD temperatures are converted and no other variables.

### Check if the file has CTD variables

A check is done to see if the file has at a minimum a pressure and a
ctd_temperature. If it doesn't, it is not converted.

### Convert to ArgoVis JSON format

Convert CF netCDF files into an ArgoVis JSON format of meta data and parameter
data. No renaming is done at this step. The following information is included in
the JSON file. There is meta data describing the cruise, parameter data listing
variables and their values, and mappings of names, reference scale and units.
The bgcMeas object contains all the data and quality codes, and the key
measurements contains a subset of pressure, ctd_temperature, ctd_salinity, and
ctd_oxygen with a quality code of 2. These are the core variables.

    Core Variables:
    pressure, ctd_temperature, ctd_salinity and bottle_salinity if ctd_salinity doesn't exist.

Meta Data:

    "DATA_CENTRE", "POSITIONING_SYSTEM", "_id", "id"
    "expocode", "cast", "section_id", "station", "btm_depth"
    "cruise_url", "netcdf_url", "data_filename", "date", "date_formatted"
    "latitude", "longitude", "roundLat", "roundLon", "strLat", "strLon", "geoLocation"

Parameter Data:

    "bgcMeas", "measurements"

Mappings and variables from the initial file before any conversions:

    "goship_names", "goship_ref_scale", "goship_units"

### Modify measurements list for source and add a measurements source key

For the measurements list, only core variables are contained in each object of
the measurements list. For bottle files, ctd files, and combined bottle and ctd
files, the core variables have their '\_btl' or '\_ctd' suffix removed. Core
values are pressure, ctd temperature, and ctd salinity. For a bottle file, if
there is no ctd salinity, then bottle salinity is used if it exists.

See below for order of core variables used when combining bottle and ctd files.

### Rename to ArgoVis vocabulary

The ArgoVis JSON formatted file is now renamed to the ArgoVis vocabulary.

For a CTD file, core variables of (pressure, ctd_temperature, ctd_salinity, and
ctd_oxygen) are renamed as shown below. a suffix of "\_ctd" is added to
variables and "\_qc" is mapped to "\_ctd_qc". Meta variables do not have a
suffix.

For a bottle file, core variables of (pressure, ctd_temperature, ctd_salinity,
and ctd_oxygen) and bottle_saliniy are renamed as shown below. a suffix of
"\_ctd" is added to variables and "\_qc" is mapped to "\_ctd_qc". Meta variables
do not have a suffix.

Additional mappings are included of "argovisReferenceScale",
"goshipArgovisNameMapping", "goshipArgovisUnitNameMapping"

The mapping argovisReferenceScale lists renamed variables and their current
reference scale. In the case of ctd temperature, if it was originally on an
IPTS-68 scale and then converted to a ITS-90 scale, the ITS-90 scale is listed
as the ArgoVis reference scale.

For goshipArgovisNameMapping, it is a mapping of variables to the ArgoVis
vocabulary. Not all variables are renamed. Variables renamed are core variables,
bottle salinity, and latitude and longitude.

    For a renamed CTD file

    "goshipArgovisNameMapping": {
        "ctd_oxygen": "doxy_ctd",
        "ctd_oxygen_qc": "doxy_ctd_qc",
        "ctd_salinity": "psal_ctd",
        "ctd_salinity_qc": "psal_ctd_qc",
        "ctd_temperature": "temp_ctd",
        "ctd_temperature_qc": "temp_ctd_qc",
        "latitude": "lat",
        "longitude": "lon",
        "pressure": "pres"
    }

    For a renamed Bottle File

    "goshipArgovisNameMapping": {
        "bottle_salinity": "salinity_btl",
        "bottle_salinity_qc": "salinity_btl_qc",
        "ctd_oxygen": "doxy_btl",
        "ctd_oxygen_qc": "doxy_btl_qc",
        "ctd_salinity": "psal_btl",
        "ctd_salinity_qc": "psal_btl_qc",
        "ctd_temperature": "temp_btl",
        "latitude": "lat",
        "longitude": "lon",
        "pressure": "pres"
    }

The reference scale mapping argovisReferenceScale lists the reference scale of
core variables.

    "argovisReferenceScale": {
        "psal_btl": "PSS-78",
        "salinity_btl": "PSS-78",
        "temp_btl": "ITS-90"
    }

### Combine Bottle and CTD profiles

The bottle and CTD profiles are combined into one by adding a suffix of "\_btl"
to bottle meta data. Common variables like expocode are not given a btl suffix.
The bgcMeas bottle and ctd objects are combined into one. The measurements
object is combined with a hierarchy of core variables from the bottle and ctd
files. The unit and reference scale mappings are combined. The name mapping is
kept separate and identified by a Btl and Ctd suffix because their goship names
are the same.

### Modify measurements list for source and add a measurements source key

## Unique identifier id

The unique identifier is a combination of <expocode>_<station>_<cast>

## To create conda environment

    Requirement: conda installed

    Modify conda_environment.yaml to set the conda environment name
    you want to use
    name: <conda environment name>

    Within the repo created, run the following command to
    create the conda environment needed
    conda env create --file conda_environment.yaml

    Then activate the Conda environment
    conda activate <conda environment name>

## To Run it

To process all years
    Program assumes start year is 1900 and end year is Dec of current year

    python convert_netcdf_to_json.py

To process a range of years use -s and -e flags
The start date is the Jan 1 of the start year and
The end date is Dec 31 of the end year

    python convert_netcdf_to_json.py -s 1998 -e 2001


## Folder for converted data

The data output is saved to converted_data. Each cruise dataset is saved as a
zip archive named with the cruise expocode.

## Notes about creation of the Conda environemnt

 This xarray page explains the IO requirements to use the xarray command "open_dataset".
 https://xarray.pydata.org/en/stable/getting-started-guide/installing.html

 This page explains the IO error that can occur without the following installed.
 https://stackoverflow.com/questions/67725531/io-backend-error-in-the-axarray-for-netcdf-file

