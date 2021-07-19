import xarray as xr
import fsspec


def main():
    data_url = "https://cchdo.ucsd.edu/data/18420/316N154_2_ctd.nc"

    # with fsspec.open(data_url) as fobj:
    #     nc = xr.open_dataset(fobj, engine='h5netcdf')

    with fsspec.open(data_url) as fobj:
        nc = xr.open_dataset(fobj, engine='h5netcdf',
                             chunks={"N_PROF": 20})

    nc.load()
    print(nc)


if __name__ == '__main__':

    main()
