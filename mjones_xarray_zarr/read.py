import sys
import os
import time, os
import s3fs
import xarray as xr


run_path = '/home/users/mjones07/tds_testing/s3_xarray_test'
with open('%s/access_cred.secret' % run_path,'r') as f:
    CREDS = {}
    for line in f:
        CREDS[line.split(',')[0].strip()] = line.split(',')[1].strip()


caringo_s3 = s3fs.S3FileSystem(anon=False,
            key=CREDS['ACCESS_KEY'],
            secret=CREDS['SECRET_KEY'],
            client_kwargs={'endpoint_url': CREDS['ENDPOINT_URL']})


zarr_path = 'mjones07/xarray_zarr_test'

store = s3fs.S3Map(root=zarr_path, s3=caringo_s3)
ds = xr.open_zarr(store=store, consolidated=True)
var = ds['var']

bytes_read = 0

for i in range(150):
    data = var[i,:,:,:].load()
    bytes_read += data.nbytes

ds.close()
