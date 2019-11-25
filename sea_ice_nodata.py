# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 14:00:21 2019

@author: disbr007

Resamples NSDIC Sea-Ice Rasters to change class values (land, coast, etc.)
to be no data value.

TO DO: Change to also download new rasters since last update
Resamples values in rasters to a new no data value.
"""

import argparse
from datetime import datetime
from ftplib import FTP
import numpy as np
import os
from osgeo import gdal, osr
from tqdm import tqdm


gdal.UseExceptions()


def update_rasters(last_update, north=True, south=True):
    '''
    Downloads any new rasters since the last update date.
    last_update: date string like '2019-07-31'
    north: update Arctic rasters
    south: update Antarctic rasters
    '''
    ## Convert last update to datetime and get year, month, day

#last_update = '2019-09-02'
#north = True
#south = False

    last_update_dt = datetime.strptime(last_update, r'%Y-%m-%d')
    last_update_year = last_update_dt.year
    last_update_month = last_update_dt.month
    last_update_day = last_update_dt.day
    
    ftp = FTP(r'sidads.colorado.edu')
    ftp.login()
    ftp.cwd(r'/DATASETS/NOAA/G02135/')


    ## Fix this - loop through each of N and S if True
#    if north:
#        ftp.cwd(r'north/daily/geotiff/')
#            
#    elif south:
#        ftp.cwd(r'south/daily/geotiff/')
    
    ## Get current directory and iterate    
    data_dir = ftp.pwd()
    
    years = ftp.nlst(data_dir)[2:] # drop '.' and '..' from list of dirs
    for year_dir in years:
        if int(year_dir) > last_update_year:
            ## Get it all
    #        ftp.cwd(r'{}/{}'.format(data_dir, year_dir))
            x = ''
            
        elif int(year_dir) == last_update_year:
    #        ftp.cwd(r'{}/{}'.format(data_dir, year_dir))
            month_dirs = ftp.nlst(r'{}/{}'.format(data_dir, year_dir))[2:]
            
            for month_dir in month_dirs:
                month = int(month_dir[:2])
            
                if month > last_update_month:
                    ## Get it all
                    print('month >', month)
            
                elif month == last_update_month:
                    print('month ==', month)
                    file_ps = ftp.nlst(r'{}/{}/{}'.format(data_dir, year_dir, month_dir))[2:]
    #                print(file_ps)
                    ## Date is part of file name - day is element 8 and 9
                    days = {x[8:10]: x for x in file_ps}
                    for day, file_n in days.items():
                        print(day)
                        if int(day) > last_update_day:
                            ## Get it all
                            file_p = r'{}/{}/{}/{}'.format(data_dir, year_dir, month_dir, file_n)
                            out_p = os.path.join(r'C:\temp\sea_ice', file_n)
                            print(r'Getting: {}/{}/{}'.format(data_dir, year_dir, file_p))
                            
                            #### TO DO: FUNCTION TO CREATE SUBDIRS IF NEEDED FOR A FILE PATH
                            with open(out_p, 'wb') as fhandle:
                                ftp.retrbinary("RETR {}".format(file_p), fhandle.write)
                        


    
    
# update_rasters('2019-01-01')


def resample_nodata(f_p, nd1, nd2, nd3, nd4, out_path, out_nodata):
    '''
    Takes the NSDIC Sea-ice .tifs and resamples the four 
    classes to be no-data values.
    f_p: file path to .tif
    nd1 - nd4: no data values (how to provide a list of args to np.where?)
    out_path: path to write resampled .tif to
    '''
    
    ## Read source and metadata
    ds = gdal.Open(f_p)
    gt = ds.GetGeoTransform()
    
    prj = osr.SpatialReference()
    prj.ImportFromWkt(ds.GetProjectionRef())
    
    x_sz = ds.RasterXSize
    y_sz = ds.RasterYSize
#    src_nodata = ds.GetRasterBand(1).GetNoDataValue()
    dtype = ds.GetRasterBand(1).DataType
#    dtype = gdal.GetDataTypeName(dtype)

    
    ## Read as array and convert no data to -9999
    ar = ds.ReadAsArray()
    ar = np.where((ar == nd1) | (ar == nd2) | (ar == nd3) | (ar == nd4), out_nodata, ar)

    
    ## Write
    # Look up table for GDAL data types - dst is the signed version of src if applicable
    signed_dtype_lut = {
            0: {'src': 'Unknown', 'dst': 0},
            1: {'src': 'Byte', 'dst': 1},
            2: {'src': 'UInt16', 'dst': 3},
            3: {'src': 'Int16', 'dst': 3},
            4: {'src': 'UInt32', 'dst': 5},
            5: {'src': 'Int32', 'dst': 5},
            6: {'src': 'Float32', 'dst': 6},
            7: {'src': 'Float64', 'dst': 7},
            8: {'src': 'CInt16', 'dst': 8},
            9:{'src': 'CInt32', 'dst': 9},
            10:{'src': 'CFloat32', 'dst': 10},
            11:{'src': 'CFloat64', 'dst': 11},
            }
    
    # Create intermediate directories
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    fmt = 'GTiff'
    driver = gdal.GetDriverByName(fmt)
    dst_dtype = signed_dtype_lut[dtype]['dst']
    dst_ds = driver.Create(out_path, x_sz, y_sz, 1, dst_dtype)
    dst_ds.GetRasterBand(1).WriteArray(ar)
    dst_ds.SetGeoTransform(gt)
    dst_ds.SetProjection(prj.ExportToWkt())
    dst_ds.GetRasterBand(1).SetNoDataValue(out_nodata)
    
    dst_ds = None



def resample_loop(sea_ice_directory, out_dir, last_update, out_nodata):
    '''
    Calls resample_nodata in a loop for every *_concetration.tif and 
    *_extent.tif in the given directory, resampling class values to
    no data.
    sea_ice_path: path to directory holding rasters. sub-directories are OK.
    out_dir: path to write resampled rasters to.
    last_update: date of last update. Rasters after this date will be 
                    resampled. e.g. '2019-07-31'
    '''
    ## Convert last update to datetime and get year, month, day
    last_update_dt = datetime.strptime(last_update, '%Y-%M-%d')
    last_update_year = last_update_dt.year
    last_update_month = last_update_dt.month
    last_update_day = last_update_dt.day
        
    ## Concentration raster no data values
    con_miss = 2550
    con_land = 2540
    con_coast = 2530
    con_pol = 2510
    
    ## Extent raster no data values
    ext_miss = 255
    ext_land = 254
    ext_coast = 253
    ext_pol = 210
    
    
    ## Loop through rasters
    for root, dirs, files in os.walk(sea_ice_directory):
        for file in tqdm(files):
            f_p = os.path.join(root, file)
            ## The raster date is part of the filename
            date = os.path.basename(f_p).split('_')[1]
            year, month, day = int(date[:4]), int(date[4:6]), int(date[6:8])
            ## If the rasters date is after last_update, resample
            if year >= last_update_year and month >= last_update_month and day > last_update_day:
                out_path = os.path.join(out_dir, os.path.relpath(os.path.join(root, file), sea_ice_directory))
                # Resample concentration rasters
                if file.endswith('_concentration_v3.0.tif'):
                    resample_nodata(con_miss, con_land, con_coast, con_pol, out_path=out_path, out_nodata=out_nodata)
                # Resample extent rasters
                if file.endswith('_extent_v3.0.tif'):
                    resample_nodata(ext_miss, ext_land, ext_coast, ext_pol, out_path=out_path, out_nodata=out_nodata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('sea_ice_directory', type=str,
                        help='Path to directory containing sea-ice rasters. Subdirectories are OK.')
    parser.add_argument('last_update_date', type=str,
                        help="""The date through which rasters have been resampled. I.E. set to 1990-01-01
                        to resample everything from 1990-01-01 to present.""")
    parser.add_argument('out_directory', type=str,
                        help='Directory to write resampled rasters to.')
    parser.add_argument('--out_nodata', type=str, default=-9999,
                        help='No data value to use for resampled rasters. Default = -9999')
    
    args = parser.parse_args()
    
    sea_ice_dir = args.sea_ice_directory
    last_update = args.last_update_date
    out_dir = args.out_directory
    out_nodata = args.out_nodata
    
    resample_loop(sea_ice_dir, out_dir=out_dir, last_update=last_update, out_nodata=out_nodata)
