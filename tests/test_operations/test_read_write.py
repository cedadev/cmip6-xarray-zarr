#!/usr/bin/env python

import pytest
import subprocess
import sys
import os
from cmip6_xarray_zarr.utils import constants as cts
test_dir = "../tests/mini-esgf-data/test_data/badc/cmip6/data/"

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
        ds = read_mfdataset(dr)
        assert(isinstance(ds, xarray.dataset))

def tearDown(zarr_path):
    """Tear down test fixtures, if any."""
    os.rmdir(zarr_path)