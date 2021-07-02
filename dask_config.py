import dask

import dask.distributed  # populate config with distributed defaults

dask.config.config

print(dask.config.config)
