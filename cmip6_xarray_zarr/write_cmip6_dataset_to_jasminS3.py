#!/usr/bin/env Python
import xarray as xr
import os
import sys
import s3fs
import json
import subprocess
import logging
import time
from utils import constants as cts

logging.basicConfig(format='[%(levelname)s]:%(message)s', level=logging.INFO)
with open(cts.s3_creds_file) as f:
    jasmin_store_credentials = json.load(f)

jasmin_s3 = s3fs.S3FileSystem(anon=False, secret=jasmin_store_credentials['secret'],
                              key=jasmin_store_credentials['token'], client_kwargs={'endpoint_url': jasmin_store_credentials['endpoint_url']})

def _setup_env():
    """
    export OMP_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export OPENBLAS_NUM_THREADS=1
    """
    env = os.environ
    env['OMP_NUM_THREADS'] = '1'
    env['MKL_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'


# _setup_env()


def setup_configs(ddir):
    logging.debug(f'Configuring {ddir}')
    zarr_path = f"cmip6-test/{ddir.strip().strip('/').replace('/', '_')}"
    chunk_rule = {'time': 4}
    
    return zarr_path, chunk_rule


def main(dr):

    logging.info(f'Scanning {dr}')
    logging.info(f'Number of files {len(os.listdir(dr))}')
    for f in os.listdir(dr):
        logging.info(f'    {f} {os.path.getsize(os.path.join(dr, f))}')


    # zarr_path, chunk_rule = setup_configs(dr.strip('/badc/cmip6/data/'))
    # logging.info(f'Writing to {zarr_path}')
    #
    # ds = xr.open_mfdataset(f'{dr}/*.nc')
    # chunked_ds = ds.chunk(chunk_rule)
    # s3_store = s3fs.S3Map(root=zarr_path, s3=jasmin_s3)
    # start = time.time()
    # chunked_ds.to_zarr(store=s3_store, mode='w', consolidated=True)
    # end = time.time()
    # time_taken = end - start
    # logging.info(f'Time taken to write dataset {dr} {time_taken}')
    # 

if __name__ == "__main__":

    drs = sys.argv[1:]
    logging.debug(f'Number of datasets {len(drs)}')

    for dr in drs:
        main(dr)