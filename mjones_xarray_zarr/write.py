import s3fs
import os
import sys
import json
import xarray as xr
import numpy as np

run_path = os.getcwd()
# Get S3 credentials
credsfile = "/home/users/rpetrie/cmip6/zarr-test/cmip6-xarray-zarr/s3_creds.json"
with open(credsfile) as f:
    jasmin_store_credentials = json.load(f)

# set size of test data
dsize = 50

# create dataset
SRC = "/badc/cmip6/data/CMIP6/CMIP/MOHC/UKESM1-0-LL/amip/r1i1p1f4/Amon/tas/gn/v20200213"
dr = SRC
print(f'[INFO] Working on: {dr}')

ds = xr.open_mfdataset(f'{dr}/*.nc')  # , parallel=False)
#
# ds = xr.Dataset({'var': (('dim0', 'dim1', 'dim2', 'dim3'), np.random.rand(dsize,dsize,dsize,dsize))},
#                             coords={'dim0': range(dsize),
#                             'dim1': range(dsize),
#                             'dim2': range(dsize),
#                             'dim3': range(dsize)})

#initialise the s3 store
caringo_s3 = s3fs.S3FileSystem(anon=False, secret=jasmin_store_credentials['secret'],
                               key=jasmin_store_credentials['token'],
                               client_kwargs={'endpoint_url': jasmin_store_credentials['endpoint_url']})

#zarr_path = 'mjones07/xarray_zarr_test'
zarr_path = f"rp-test/zarr-test-1"
store = s3fs.S3Map(root=zarr_path, s3=caringo_s3)

#set chunking for efficient read
#ds = ds.chunk({'dim0':1,'dim1':dsize, 'dim2':dsize, 'dim3':dsize})
chunk_rule = {'time': 4}
chunked_ds = ds.chunk(chunk_rule)

# push to s3
ds.to_zarr(store=store, consolidated=True, mode='w')
ds.close() 