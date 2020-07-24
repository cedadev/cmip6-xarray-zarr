#!/usr/bin/env python

import pytest
import subprocess
import sys
import os
import xarray as xr
from cmip6_xarray_zarr.utils import constants as cts
from cmip6_xarray_zarr import zarr_s3_rw as c6xrz

test_dir = "../mini-esgf-data/test_data/badc/cmip6/data/"
datasets = ['CMIP6.CMIP.IPSL.IPSL-CM6A-LR.historical.r1i1p1f1.SImon.siconc.gn',]
drs = [ os.path.join(test_dir, d.replace('.', '/'), 'latest') for d in datasets ]
zarr_path = f'{cts.LOCAL_OUTPUT_DIR}/test.zarr'

def setUp():
    """Set up test fixtures, if any."""
    env = os.environ
    env['OMP_NUM_THREADS'] = '1'
    env['MKL_NUM_THREADS'] = '1'
    env['OPENBLAS_NUM_THREADS'] = '1'


def test_read_mfdataset():
    for dr in drs:
        ds = c6xrz.read_mfdataset(dr)
        print(ds.siconc.values[120][3][1])
        assert(isinstance(ds, xr.Dataset))


def test_write_dataset_to_disk():
    for dr in drs:
        ds = c6xrz.read_mfdataset(dr)
        id = '-'.join(dr.split('/')[9:14])
        zarr_path = f'{cts.LOCAL_OUTPUT_DIR}/test_{id}.zarr'
        print(zarr_path)
        written = c6xrz.write_dataset_to_disk(ds, zarr_path)
        assert(written)

def tearDown():
    """Tear down test fixtures, if any."""
    for dr in drs:
        id = '-'.join(dr.split('/')[9:14])
        zarr_path = f'{cts.LOCAL_OUTPUT_DIR}/test_{id}.zarr'
        subprocess.call(f'rm -fr {zarr_path}'.split())

def test_write_to_jasmin_s3():

    for dr in drs:
        id = '-'.join(dr.split('/')[9:14])
        zarr_path = f'{cts.LOCAL_OUTPUT_DIR}/test_{id}_2.zarr'
        ds = c6xrz.read_mfdataset(dr)
        s3_store = c6xrz.write_to_jasmin_s3(cts.s3_creds_file, zarr_path, ds)
        recovered_ds = c6xrz.read_from_jasmin_s3(s3_store)
        print(recovered_ds.siconc.values[120][3][1])
        assert (isinstance(recovered_ds, xr.Dataset))

setUp()
test_read_mfdataset()
test_write_dataset_to_disk()
test_write_to_jasmin_s3()
tearDown()