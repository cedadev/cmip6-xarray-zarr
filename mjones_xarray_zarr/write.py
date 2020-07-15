import s3fs
import os, sys
import xarray as xr
import numpy as np

run_path = os.getcwd()
# Get S3 credentials
with open('%s/mjones_xarray_zarr/access_cred.secret' % run_path,'r') as f:
    CREDS = {}
    for line in f:
        CREDS[line.split(',')[0].strip()] = line.split(',')[1].strip()

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
            key=CREDS['ACCESS_KEY'],
            secret=CREDS['SECRET_KEY'],
            client_kwargs={'endpoint_url': CREDS['ENDPOINT_URL']})

zarr_path = 'mjones07/xarray_zarr_test'

store = s3fs.S3Map(root=zarr_path, s3=caringo_s3)

#set chunking for efficient read
ds = ds.chunk({'dim0':1,'dim1':dsize, 'dim2':dsize, 'dim3':dsize})
# push to s3
ds.to_zarr(store=store, consolidated=True, mode='w')
ds.close() 