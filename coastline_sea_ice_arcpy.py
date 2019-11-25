# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 14:15:04 2019

@author: disbr007
"""

import arcpy
import logging
import os
import numpy as np
import pickle
import sys


def coastline_sea_ice(src, initial_candidates, final_candidates, wd, gdb, ice_threshold, update_luts=False):
    #### Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    
    def create_daily_dict(year_start=1978, year_stop=2020):
        ## Initialize empty dictionary with entries for each year, month, and day
        years = [str(year) for year in range(year_start, year_stop+1)]
        months = [str(month) if len(str(month))==2 else '0'+str(month) for month in range(1, 13)]
        days = [str(day) if len(str(day))==2 else '0'+str(day) for day in range(1,32)]
        empty_index = {year: {month: {day: None for day in days} for month in months} for year in years}
        
        return empty_index
    
    
    def get_raster_paths(sea_ice_dir, update=False):
        '''
        Gets all of the raster paths for the given path and puts them in a dictionary
        sorted by year, month, day.
        sea_ice_dir: directory to parse
        '''
        raster_index = create_daily_dict()
        
        for root, dirs, files in os.walk(sea_ice_dir):
            for f in files:
                # Only add concentration rasters
                if f.endswith('_concentration_v3.0.tif'):
                    date = f.split('_')[1] # f format: N_ N_19851126_concentration_v3.0.tif
                    year = date[0:4]
                    month = date[4:6]
                    day = date[6:8]
                    raster_index[year][month][day] = os.path.join(root, f)
    
        ## Remove empty dictionary entries
        # Remove empty days
        for year_k, year_v in raster_index.copy().items():
            for month_k, month_v in raster_index[year_k].copy().items():
                for day_k, day_v in raster_index[year_k][month_k].copy().items():
                    if day_v == None:
                        del raster_index[year_k][month_k][day_k]
        # Remove empty months
        for year_k, year_v in raster_index.copy().items():
            for month_k, month_v in raster_index[year_k].copy().items():
                if not month_v:
                    del raster_index[year_k][month_k]
        # Remove empty years
        for year_k, year_v in raster_index.copy().items():
            if not year_v:
                del raster_index[year_k]
                
        return raster_index
    
    
    def create_raster_lut(pole, year_start=1978, year_stop=2020, update=False):
        '''
        Creates a lookup dictionary for each raster in the sea ice
        raster directory.
        pole: 'arctic' or 'antarctic' to determine which rasters to sample
        '''
        ## Create a 'arctic_sea_ice_path' and 'antarctic_sea_ice_path - use lat to determine which to sample
        arctic_pickle_path = os.path.join(wd, r'pickles\arc_sea_ice_concentraion_index.pkl')
        antarctic_pickle_path = os.path.join(wd, r'pickles\ant_sea_ice_concentraion_index.pkl')
        if update == False:
    #        raster_index = pd.read_pickle(pickle_path)
            if pole == 'arctic':
                with open(arctic_pickle_path, 'rb') as handle:
                    raster_index = pickle.load(handle)
            elif pole == 'antarctic':
                with open(antarctic_pickle_path, 'rb') as handle:
                    raster_index = pickle.load(handle)
        else:
            ## Concentration raster locations
            arctic_ice_dir = os.path.join(wd, r'noaa_sea_ice\north\resampled_nd\daily\geotiff')
            antarctic_ice_dir = os.path.join(wd, r'noaa_sea_ice\south\resampled_nd\daily\geotiff')
            
    #        logger.info('Creating look-up table of sea-ice rasters by date...')
            ## Walk rasters extracting date information
            if pole == 'arctic':
                raster_index = get_raster_paths(arctic_ice_dir)
                with open(arctic_pickle_path, 'wb') as handle:
                    pickle.dump(raster_index)
            elif pole == 'antarctic':
                raster_index = get_raster_paths(antarctic_ice_dir)
                with open(arctic_pickle_path, 'wb') as handle:
                    pickle.dump(raster_index)
            else:
                logging.ERROR('Unrecognized pole argument while creating raster lookup table: {}'.format(pole))
            
        return raster_index
    
    
    def choose_pole(y):
        '''
        Returns the raster look-up-table appropriate for given y (latitude)
        '''
        if y > 50.0:
            pole = 'arctic'
        elif y < -50.0:
            pole = 'antarctic'
        else:
            pole = None
        
        return pole
    
    
    #### Load raster look up tables - dictionaries sorted by date [year][month][day] = daily_raster_path
    logger.info('Loading raster look-up-tables.')
    arctic_lut = create_raster_lut(pole='arctic', update=update_luts)
    ant_lut = create_raster_lut(pole='antarctic', update=update_luts)
    
        
    #### Loop through candidates, determine appropriate look-up-table, assign path to new field (or just sample path)
    ## Copy candidates to new feature class that will have sea-ice - eventually not needed
    logger.info('Copying candidates feature class.')
    ## Name of intermediate feature class - in memory
    sea_ice_fc = '{}_all_ice'.format(src) ## fix to write to memory, getting CopyFeatures error)
    arcpy.CopyFeatures_management(initial_candidates, sea_ice_fc)

    ## Date column based on src
    date_col_lut = {
            'dg': 'acqdate',
            'mfp': 'acq_time',
            'mfp_test': 'acq_time',
            'nasa': 'ACQ_TIME',
            'oh': 'acq_time'}
    
    ## Add count field to output feature class
    fields = [field.name for field in arcpy.ListFields(sea_ice_fc)]
    concentration_field = 'sea_ice_concentration'
    if concentration_field not in fields:
        arcpy.AddField_management(sea_ice_fc,
                              field_name=concentration_field,
                              field_type='DOUBLE')
    
    
    #### TO DO: sort data, interpolate raster
    logger.info('Sampling rasters for ice concentration...')
    with arcpy.da.UpdateCursor(sea_ice_fc, ["SHAPE@XY", date_col_lut[src], "OBJECTID", concentration_field]) as cursor:
        for i, row in enumerate(cursor):
            ## Get latitude to determine whether to sample arctic, antarctic, or neither
    #        print(row[2])
            if i % 10000 == 0:
                logging.info('Calculating sea ice on feature number: {}...'.format(i))
            x, y = row[0][0], row[0][1]
    
            pole = choose_pole(y)
            year, month, day = row[1][:10].split('-')
            
            if pole != None:
                if pole == 'arctic':
                    lut = arctic_lut
                    epsg = 3413 # NSIDC Polar Stereographic North (same as sea-ice rasters)
                elif pole == 'antarctic':
                    lut = ant_lut
                    epsg = 3412 # NSIDC Polar Stereographic South
                
                ## Coordinates of footprint centroid in projection of raster
                center_prj = arcpy.PointGeometry(arcpy.Point(x, y),arcpy.SpatialReference(4326)).projectAs(arcpy.SpatialReference(epsg))
                x_prj, y_prj = center_prj.centroid.X, center_prj.centroid.Y
                
                raster_p = lut[year][month][day]
                raster = arcpy.Raster(raster_p)
                
                cell_height, cell_width = raster.meanCellHeight, raster.meanCellWidth
                
                lower_left = arcpy.Point(X=x_prj-(2*cell_width), Y=y_prj-(2*cell_height))
                window_shape = (4,4)
                ncols = window_shape[0]
                nrows = window_shape[1]            
    #            lower_left = arcpy.Point(raster.extent.XMin, raster.extent.YMin)
                valid_values = False
                while valid_values == False:
                    sea_ice_arr = arcpy.RasterToNumPyArray(raster_p, 
                                                              lower_left_corner=lower_left, 
                                                              ncols=ncols, 
                                                              nrows=nrows,
                                                              nodata_to_value=None
                                                              )
                    sea_ice_arr = np.where(sea_ice_arr == -9999, np.nan, sea_ice_arr)
                    
                    if False in np.isnan(sea_ice_arr):
                        concentration = int(np.nanmean(sea_ice_arr) / 10)
                        row[3] = concentration
    #                    print(concentration)
                        valid_values = True
                    else:
                        if nrows > 10:
                            valid_values = True
                            concentration = 0
    #                    print('growing...')
                        nrows += 1
                        ncols += 1
                cursor.updateRow(row)
            ## Nonpolar
            else:
    #            print('nonpolar')
                row[3] = 0
                cursor.updateRow(row)
                
    logging.info('Writing {}...'.format(final_candidates))
    where = """{} <= {}""".format(concentration_field, ice_threshold)
    selection = arcpy.MakeFeatureLayer_management(sea_ice_fc, final_candidates, where_clause=where)
    arcpy.CopyFeatures_management(selection, out_feature_class=final_candidates)
