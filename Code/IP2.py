#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import os
import re
import sys
import tempfile

import fiona
from fiona import transform
import numpy as np
from osgeo import gdal, osr
from pyproj import Transformer
import rasterio
import rasterio.mask
from rasterio.io import MemoryFile

#delete later
import time

#initalize dictionary to return and generic metadata
OUTDATA = {}
OUTDATA['metadata'] = []
OUTDATA['metadata'].append({
    "Temporal Resolution": "1",
    "Temporal Units": "1",
    "Temperature Units": "degrees C",
    "Wind Direction Units": "Wind direction (from which blowing) [deg true]",
    "Wind Speed Units": "m/s"
})

def reprojectLine(geojson_path, raster_path):
    toReturn = []
    #get wkt projection definition from grib
    ds = gdal.Open(raster_path)
    inputSRS_wkt = ds.GetProjection()

    with open(geojson_path) as f:
        gj = json.load(f)

    #for each line in the geojson
    for geom in range (0, len(gj['features'])):
        #for each point in the line
        for point in gj['features'][geom]['geometry']['coordinates']:
            #get the point
            x = point[1]
            y = point[0]

            #reproject the point
            srs = osr.SpatialReference()
            srs.ImportFromWkt(inputSRS_wkt)
            #assume all geojson inputs have crs epsg:4326
            transformer = Transformer.from_crs("epsg:4326", srs.ExportToProj4())
            _x, _y = transformer.transform(x, y)

            toReturn.append([_x, _y])
    return toReturn

def reprojectPoly(geojson_path, raster_path):
    ret = []
    toReturn = []
    #get wkt projection definition from grib
    ds = gdal.Open(raster_path)
    inputSRS_wkt = ds.GetProjection()

    with open(geojson_path) as f:
        gj = json.load(f)

    #for each line in the geojson
    for geom in range (0, len(gj['features'])):
        #for each point in the line
        for line in range (0, len(gj['features'][geom]['geometry']['coordinates'])):
            for point in range(0, len(gj['features'][geom]['geometry']['coordinates'][line])):
                #get the point
                x = gj['features'][geom]['geometry']['coordinates'][line][point][1]
                y = gj['features'][geom]['geometry']['coordinates'][line][point][0]

                #reproject the point
                srs = osr.SpatialReference()
                srs.ImportFromWkt(inputSRS_wkt)
                #assume all geojson inputs have crs epsg:4326
                transformer = Transformer.from_crs("epsg:4326", srs.ExportToProj4())
                _x, _y = transformer.transform(x, y)

                ret.append([_x, _y])
    toReturn.append(ret)
    return toReturn

#get crs from input geojson
def getCRS(geoJSON_path):
    with open(geoJSON_path) as f:
        gj = json.load(f)
    crs = gj["crs"]
    crs = str(crs)
    return crs
    
#get the prediction hour by searching the raster's filename
def getTime(raster_path):
    time_ = re.findall("P\d{3}", raster_path)
    time_ = str(time_)
    time_ = time_.replace("'","")
    time_ = time_[2:5]
    time_ = int(time_)
    return time_

#delete time model run info grab when running on server

    
#get the value at a specific input point in a point query
def getPoint(raster_list, geoJSON_path, count):
    toReturn = {}
    i = 0
    
    for raster_path in raster_list:
        #check if the input geojson crs matches the raster crs and reproject if needed and get input point geometry
        coords = reproject(geoJSON_path, raster_path)
        
        #get the prediction hour by searching the raster's filename
        time_ = getTime(raster_path)
        
        #setup and initialize vars for differentiating between temp, wdir, wspeed queries
        if "TMP" in raster_path:
            dataType = "Temperature Data"
        if "WDIR" in raster_path:
            dataType = "Wind Direction Data"
        if "WIND" in raster_path:
            dataType = "Wind Speed Data"
        
        #get inital lat/long coordinates
        x = coords[0][0]
        y = coords[0][1]

        #tranform from lat/long coordinates to their corresponding (x,y) pixel in the raster
        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        geotransform = ds.GetGeoTransform()
        origin_x = geotransform[0]
        origin_y = geotransform[3]
        width = geotransform[1]
        height = geotransform[5]
        x = int((x - origin_x) / width)
        y = int((y - origin_y) / height)        
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray()
        toReturn[i] = [time_, coords[0][0], coords[0][1], arr[y][x], dataType]
        i += 1
        
    return toReturn

#get the values along an input line in a line query
def getLine(raster_list, geoJSON_path):
    toReturn = {}
    i = 0
    with open(geoJSON_path) as f:
        gj = json.load(f)
    inputLine = gj['features'][0]['geometry']['coordinates']    
    inputLine = str(inputLine)
    inputLine = inputLine.replace(" ", "")
    
    for raster_path in raster_list:        
        #setup and initialize vars for differentiating between temp, wdir, wspeed queries
        temp = False
        direc = False
        speed = False
        if "TMP" in raster_path:
            temp = True
        if "WDIR" in raster_path:
            direc = True
        if "WIND" in raster_path:
            speed = True
        
        #get the prediction hour by searching the raster's filename
        time_ = getTime(raster_path)
    
        #Open Raster
        with rasterio.open(raster_path) as src:
            #check if the input geojson crs matches the raster crs and reproject if needed
            shapes = []
            shapes.append({
                'type': 'LineString',
                'coordinates': reprojectLine(geoJSON_path, raster_path)
            })
            #Clip the raster
            out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
            out_meta = src.meta            
            #Setup to save and write the clipped raster in memory
            with MemoryFile() as memfile:
                with memfile.open(driver = "GTiff", height = out_image.shape[1], width = out_image.shape[2], count=1, 
                              dtype=rasterio.float64, transform = out_transform) as dataset:
                    #write the clipped raster to memory
                    dataset.write(out_image)
                    #read the clipped raster from memory
                    ds = dataset.read()
                    #remove the zero values from the bounding box surrounding the line in the raster
                    ds = ds[ds!=0]
                    toReturn[i] = [ds, temp, direc, speed, time_, inputLine]
                    i += 1
        
    return toReturn

#get the summary statistics for an input polygon in polygon queries    
def summStatsPoly(raster_list, geoJSON_path):
    #setup returns
    tempmins = {}
    tempmaxs = {}
    tempmeans = {}
    
    dirmins = {}
    dirmaxs = {}
    dirmeans = {}
    
    speedmins = {}
    speedmaxs = {}
    speedmeans = {}
        
    for raster_path in raster_list:
        #Open Raster
        with rasterio.open(raster_path) as src:
            #check if the input geojson crs matches the raster crs and reproject if needed
            shapes = []
            shapes.append({
                'type': 'Polygon',
                'coordinates': reprojectPoly(geoJSON_path, raster_path)
            })
            #Clip the raster
            out_image, out_transform = rasterio.mask.mask(src, shapes, crop=True)
            out_meta = src.meta
            #Setup to save and write the clipped raster in memory
            with MemoryFile() as memfile:
                with memfile.open(driver = "GTiff", height = out_image.shape[1], width = out_image.shape[2], count=1, 
                              dtype=rasterio.float64, transform = out_transform) as dataset:
                    #write the clipped raster to memory
                    dataset.write(out_image)
                    #read the clipped raster from memory
                    ds = dataset.read()
                    #create summary stats
                    minVal = np.min(ds, axis = None)
                    maxVal = np.max(ds, axis = None)
                    meanVal = np.mean(ds, axis = None)
                    #get the prediction hour by searching the raster's filename
                    time_ = getTime(raster_path)
                    #append summary stats and time_ to corresponding list
                    if "TMP" in raster_path:
                        tempmins[time_] = minVal
                        tempmaxs[time_] = maxVal
                        tempmeans[time_] = meanVal
                    if "WDIR" in raster_path:
                        dirmins[time_] = minVal
                        dirmaxs[time_] = maxVal
                        dirmeans[time_] = meanVal
                    if "WIND" in raster_path:
                        speedmins[time_] = minVal
                        speedmaxs[time_] = maxVal
                        speedmeans[time_] = meanVal

    return tempmins, tempmaxs, tempmeans, dirmins, dirmaxs, dirmeans, speedmins, speedmaxs, speedmeans 
        
def polyOut(dic, stringName):
    toReturn = []
    for key in dic.keys():
        toReturn.append({
            'Prediction Hour': key,
            stringName: dic[key]
        })
    return toReturn

def pointOut(stringName, key):
    toReturn = []
    toReturn.append({
        "Prediction Hour": key[0],
        stringName: key[3]
            })
    return toReturn

def writeOutput(features, poly, line, point):
    #initalize dictionary to return and generic metadata
    
    #prepare line ouput
    if line and not poly and not point:
        #append the inital queried geometry
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": features[0][1][5]
        })

        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []
        
        
        for item in features:
            for key in item.keys():
                if item[key][1] == True:
                    OUTDATA['Temperature Data'].append({
                        "Prediction Hour": item[key][4],
                        'Temperature Observation': item[key][0].tolist()
                    })
                if item[key][2] == True:
                    OUTDATA['Wind Direction Data'].append({
                        "Prediction Hour": item[key][4],
                        'Wind Direction Observation': item[key][0].tolist()
                    })
                if item[key][3] == True:
                    OUTDATA['Wind Speed Data'].append({
                        "Prediction Hour": item[key][4],
                        'Wind Speed Observation': item[key][0].tolist()
                    })
                    
    #prepare polygon output
    if poly:
        #append the inital queried geometry
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": None
        })

        OUTDATA['Min Temperature Data'] = []
        OUTDATA['Max Temperature Data'] = []
        OUTDATA['Mean Temperature Data'] = []
        
        OUTDATA['Min Wind Direction Data'] = []
        OUTDATA['Max Wind Direction Data'] = []
        OUTDATA['Mean Wind Direction Data'] = []
        
        OUTDATA['Min Wind Speed Data'] = []
        OUTDATA['Max Wind Speed Data'] = []
        OUTDATA['Mean Wind Speed Data'] = []
        for item in features:
            for i in range(0, len(item)):
                value = item[i]
                if i == 0:
                    OUTDATA['Min Temperature Data'].append(polyOut(value, "Min Temperature"))                          
                if i == 1:
                    OUTDATA['Max Temperature Data'].append(polyOut(value, "Max Temperature"))
                if i == 2:
                    OUTDATA['Mean Temperature Data'].append(polyOut(value, "Mean Temperature"))
                if i == 3:
                    OUTDATA['Min Wind Direction Data'].append(polyOut(value, "Min Wind Direction"))
                if i == 4:
                    OUTDATA['Max Wind Direction Data'].append(polyOut(value, "Max Wind Direction"))
                if i == 5:
                    OUTDATA['Mean Wind Direction Data'].append(polyOut(value, "Mean Wind Direction"))
                if i == 6:
                    OUTDATA['Min Wind Speed Data'].append(polyOut(value, "Min Wind Speed"))
                if i == 7:
                    OUTDATA['Max Wind Speed Data'].append(polyOut(value, "Max Wind Speed"))
                if i == 8:
                    OUTDATA['Mean Wind Speed Data'].append(polyOut(value, "Mean Wind Speed"))
                    
    #prepare point output
    if point:
        #append the inital queried geometry
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": (features[0][1][1], features[0][1][2])
        })

        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []
        for item in features:
            for key in item.keys():
                if 'Temperature Data' in item[key][4]:
                    OUTDATA['Temperature Data'].append(pointOut("Temperature Observation", item[key]))
                if 'Wind Direction Data' in item[key][4]:
                    OUTDATA['Wind Direction Data'].append(pointOut("Wind Direction Observation", item[key]))
                if 'Wind Speed Data' in item[key][4]:
                    OUTDATA['Wind Speed Data'].append(pointOut("Wind Speed Observation", item[key]))
    
    #write the ouput
    with open('outGeoJSON2.geojson', 'w') as outfile:
        json.dump(OUTDATA, outfile, indent=2)

if __name__ == '__main__':
    start_time = time.time()
    
    #parse input
    n = len(sys.argv[1])
    #split first arguement into list of grib file paths
    gribPaths = sys.argv[1][1:n-1] 
    gribPaths = gribPaths.split(', ')
    #get polygon/line/point to clip
    geoJSONPath = sys.argv[2]
    
    #setup variables for differentiating between point, line, polygon calls and other needed vars
    count = 0
    features = []
    poly = False
    line = False
    point = False
    
    #determine the query type between point, line, polygon and call the appropriate function 
    with open(geoJSONPath) as f:
        indata = json.load(f)
    for feature in indata['features']:
        if feature['geometry']['type'] == "Polygon" or feature['geometry']['type'] == "MultiPolygon":
            poly = True
            features.append(summStatsPoly(gribPaths, geoJSONPath))
            break
        elif feature['geometry']['type'] == "LineString" or feature['geometry']['type'] == "MultiLineString":
            line = True
            features.append(getLine(gribPaths, geoJSONPath))
            break
        elif feature['geometry']['type'] == "Point" or feature['geometry']['type'] == "MultiPoint":
            point = True
            features.append(getPoint(gribPaths, geoJSONPath, count))
            count += 1
    
    #after putting necessary data in features call the writing output file function
    writeOutput(features, poly, line, point)
    end_time = time.time()
    print("Run time: ", (end_time - start_time))

    """
    remove timing before final commit and submission
    """  
    
"""
IP2.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "B:/linetest.geojson"
IP2.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "B:/PolyTest.geojson"
IP2.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "B:/PointTest.geojson"
"""

