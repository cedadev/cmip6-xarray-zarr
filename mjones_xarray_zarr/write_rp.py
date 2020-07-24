import s3fs
import os, sys
import xarray as xr
import numpy as np
import json

run_path = os.getcwd()
# Get S3 credentials
creds_file = "/home/users/rpetrie/cmip6/zarr-test/cmip6-xarray-zarr/s3_creds.json"
with open(creds_file) as f:
    CREDS = json.load(f)

#set size of test data
dsize = 50

# create dataset
ds = xr.Dataset({'var': (('dim0', 'dim1', 'dim2', 'dim3'), np.random.rand(dsize,dsize,dsize,dsize))},
                            coords={'dim0': range(dsize),
                            'dim1': range(dsize),
                            'dim2': range(dsize),
                            'dim3': range(dsize)})

#initialise the s3 store
caringo_s3 = s3fs.S3FileSystem(anon=False,
            key=CREDS['token'],
            secret=CREDS['secret'],
            client_kwargs={'endpoint_url': CREDS['endpoint_url']})

zarr_path = 'rp-mjcopy/xarray_zarr_test'

store = s3fs.S3Map(root=zarr_path, s3=caringo_s3)

#set chunking for efficient read
ds = ds.chunk({'dim0':1,'dim1':dsize, 'dim2':dsize, 'dim3':dsize})
# push to s3
ds.to_zarr(store=store, consolidated=True, mode='w')
ds.close() 