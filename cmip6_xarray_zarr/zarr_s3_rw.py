#!/usr/bin/env Python

import xarray as xr
import os
import s3fs
import json
import subprocess
from utils import constants as cts


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

_setup_env()


def read_mfdataset(dr):
    """
    Read a multifile dataset with xarray
    :param dr: valid directory containing one or more valid CMIP6 NetCDF files
    :return: xarray dataset
    """
    print(f'[INFO] Working on: {dr}')
    ds = xr.open_mfdataset(f'{dr}/*.nc', engine='netcdf4', combine='by_coords')  # , parallel=False)
    return ds


def write_dataset_to_disk(dataset, filename):
    """
    Write an xarray dataset to a group workspace
    :param dataset: Xarray dataset
    :return: path to Zarr file
    """
    chunk_rule = {'time': 4}
    chunked_ds = dataset.chunk(chunk_rule)
    print(f'[INFO] Chunk rule: {chunk_rule}')
    chunked_ds.to_zarr(filename)
    print(f'[INFO] Wrote: {filename}')

    return True


def read_dataset_from_disk(output_path):
    """
    Read Xarray dataset from disk using Zarr
    :param output_path: Zarr file path
    :return: Xarray dataset
    """
    ds_read = xr.open_zarr(output_path)
    print(f'[INFO] Read {output_path}')
    return ds_read


def write_to_jasmin_s3(creds_file, zarr_path, ds):
    """
    Write to JASMIN S3 storage
    :param creds_file: JSON credentials file to JASMIN S3 storage
    :param zarr_path: path to write to on S3
    :param ds: xarray dataset
    :return: S3 storage
    """
    with open(creds_file) as f:
        jasmin_store_credentials = json.load(f)

    jasmin_s3 = s3fs.S3FileSystem(anon=False,
                                  secret=jasmin_store_credentials['secret'],
                                  key=jasmin_store_credentials['token'],
                                  client_kwargs={'endpoint_url': jasmin_store_credentials['endpoint_url']}
                                  )
    s3_store = s3fs.S3Map(root=zarr_path, s3=jasmin_s3)
    ds.to_zarr(store=s3_store, mode='w', consolidated=True)
    ds.close()
    return s3_store


def read_from_jasmin_s3(s3_store):
    """
    Read data back from JASMIN S3 Caringo store

    :param store: JASMIN S3 zarr store
    :return: Xarray dataset
    """

    recovered = xr.open_zarr(store=s3_store, consolidated=True)
    return recovered


def main():

    dr = "tests/mini-esgf-data/test_data/badc/cmip6/data/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/historical/r1i1p1f1/SImon/siconc/gn/latest/"
    ds = read_mfdataset(dr)
    print(ds.siconc.values[120][3][1])
    print(f'{cts.LOCAL_OUTPUT_DIR}/test.zarr')
    zarr_path = f'{cts.LOCAL_OUTPUT_DIR}/test.zarr'
    write_to_disk_ok = write_dataset_to_disk(ds, zarr_path)
    ds_read = read_dataset_from_disk(zarr_path)
    print(ds_read.siconc.values[120][3][1])
    subprocess.call(f"rm -fr {zarr_path}".split())
    s3_zarr_path = f"mjones07/zarr-test-12"
    zarr_s3_store = write_to_jasmin_s3(cts.s3_creds_file, s3_zarr_path, ds)
    s3_read = read_from_jasmin_s3(zarr_s3_store)
    print(s3_read.siconc.values[120][3][:])




if __name__ == '__main__':
    main()
