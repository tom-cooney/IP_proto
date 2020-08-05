#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#open files and clip rasters
import fiona
from fiona import transform
import rasterio
import rasterio.mask
#write/read rasters in memory
from rasterio.io import MemoryFile
import tempfile
#calculate zonal stats for polygons
import numpy as np
#get arguements when called from command line and remove repreojected goejson from disk
import os, sys
#read input geojson and write output file
import json
#reprojecitng vector layers and getting (lat, long) transformation to pixel(x,y)
from osgeo import gdal
#regex searches of grib filenames to get forecast hour
import re
import pyogr2ogr
import uuid

#delete later
import time

#get crs from input geojson
def getCRS(geoJSON_path):
    with open(geoJSON_path) as f:
        gj = json.load(f)
    crs = gj["crs"]
    crs = str(crs)
    return crs
    
#check if the input geojson crs matches the raster crs and reproject if needed
def checkCRSPoly(crs, geoJSON_path, directory_for_files):
    if "4326" in crs:
        with fiona.open(geoJSON_path, "r") as shapefile:
            shapes = [feature["geometry"] for feature in shapefile]
    elif "4326" not in crs:
        pyogr2ogr.main(["","-t_srs", "EPSG:4326", "-f", "GeoJSON", '-nln', 'custom_name_here', directory_for_files, "B:/Canada-d8-100m/Canada-d8-100m.geojson"])
        with fiona.open(directory_for_files) as shapefile:            
            shapes = [feature["geometry"] for feature in shapefile]
    return shapes

#check if the input geojson crs matches the raster crs and reproject if needed
def checkCRSLine(crs, geoJSON_path, directory_for_files):
    with open(geoJSON_path) as f:
        gj = json.load(f)
    inputLine = gj['features'][0]['geometry']['coordinates']
    if "4326" in crs:
        with fiona.open(geoJSON_path, "r") as shapefile:
            shapes = [feature["geometry"] for feature in shapefile]
    elif "4326" not in crs:
        pyogr2ogr.main(["","-t_srs", "EPSG:4326", "-f", "GeoJSON", '-nln', 'custom_name_here', directory_for_files, "B:/Canada-d8-100m/Canada-d8-100m.geojson"])
        with fiona.open(directory_for_files) as shapefile:            
            shapes = [feature["geometry"] for feature in shapefile]
    return shapes, inputLine

#check if the input geojson crs matches the raster crs and reproject if needed
def checkCRSPoint(crs, geoJSON_path, directory_for_files, count):
    with open(geoJSON_path) as f:
        gj = json.load(f)
    crs = gj["crs"]
    crs = str(crs)
    if "4326" in crs:
        coords = gj['features'][count]['geometry']['coordinates']
        coords = str(coords)
        coords = coords.replace("[","")
        coords = coords.replace("]","")
        coords = coords.split(",")
    elif "4326" not in crs:
        pyogr2ogr.main(["","-t_srs", "EPSG:4326", "-f", "GeoJSON", '-nln', 'custom_name_here', directory_for_files, "B:/Canada-d8-100m/Canada-d8-100m.geojson"])
        with fiona.open(directory_for_files) as shapefile:            
            coords = gj['features'][count]['geometry']['coordinates']
            coords = str(coords)
            coords = coords.replace("[","")
            coords = coords.replace("]","")
            coords = coords.split(",")
    return coords
    

#get the prediction hour by searching the raster's filename
def getTime(raster_path):
    time_ = re.findall("P\d{3}", raster_path)
    time_ = str(time_)
    time_ = time_.replace("'","")
    time_ = time_[2:5]
    time_ = int(time_)
    return time_
    
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
    
    #setup temp directory for reprojected geojson
    directory_for_files = str(uuid.uuid4())
    
    #get crs from input geojson
    crs = getCRS(geoJSON_path)
    
    #check if the input geojson crs matches the raster crs and reproject if needed
    shapes = checkCRSPoly(crs, geoJSON_path, directory_for_files)
    
    for raster_path in raster_list:        
        #Open Raster
        with rasterio.open(raster_path) as src:            
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
                        
    if "4326" not in crs:
        os.remove(directory_for_files)

    return tempmins, tempmaxs, tempmeans, dirmins, dirmaxs, dirmeans, speedmins, speedmaxs, speedmeans 

#get the values along an input line in a line query
def getLine(raster_list, geoJSON_path):
    toReturn = {}
    i = 0
    
    #setup temp directory for reprojected geojson
    directory_for_files = str(uuid.uuid4())
    
    #get crs from input geojson
    crs = getCRS(geoJSON_path)
    
    #check if the input geojson crs matches the raster crs and reproject if needed and get input line geometry
    shapes, inputLine = checkCRSLine(crs, geoJSON_path, directory_for_files)
    
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
                    
    if "4326" not in crs:
        os.remove(directory_for_files)
        
    return toReturn

#get the value at a specific input point in a point query
def getPoint(raster_list, geoJSON_path, count):
    toReturn = {}
    i = 0
    
    #setup temp directory for reprojected geojson
    directory_for_files = str(uuid.uuid4())
    
    #get crs from input geojson
    crs = getCRS(geoJSON_path)

    #check if the input geojson crs matches the raster crs and reproject if needed and get input point geometry
    coords = checkCRSPoint(crs, geoJSON_path, directory_for_files, count)
    
    for raster_path in raster_list:
        
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
        x = float(coords[0])
        y = float(coords[1])

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
        toReturn[i] = [time_, coords[0], coords[1], arr[y][x], dataType]
        i += 1
        
    if "4326" not in crs:
        os.remove(directory_for_files)
    return toReturn
    
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
    outdata = {}
    outdata['metadata'] = []
    outdata['metadata'].append({
        "Temporal Resolution": "1",
        "Temporal Units": "1",
        "Temperature Units": "degrees C",
        "Wind Direction Units": "Wind direction (from which blowing) [deg true]",
        "Wind Speed Units": "m/s"
    })
    
    #prepare line ouput
    if line and not poly and not point:
        #append the inital queried geometry
        outdata['features'] = []
        outdata['features'].append({
            "geometry": features[0][1][5]
        })

        outdata['Temperature Data'] = []
        outdata['Wind Direction Data'] = []
        outdata['Wind Speed Data'] = []
        
        
        for item in features:
            for key in item.keys():
                if item[key][1] == True:
                    outdata['Temperature Data'].append({
                        "Prediction Hour": item[key][4],
                        'Temperature Observation': item[key][0].tolist()
                    })
                if item[key][2] == True:
                    outdata['Wind Direction Data'].append({
                        "Prediction Hour": item[key][4],
                        'Wind Direction Observation': item[key][0].tolist()
                    })
                if item[key][3] == True:
                    outdata['Wind Speed Data'].append({
                        "Prediction Hour": item[key][4],
                        'Wind Speed Observation': item[key][0].tolist()
                    })
                    
    #prepare polygon output
    if poly:
        #append the inital queried geometry
        outdata['features'] = []
        outdata['features'].append({
            "geometry": None
        })

        outdata['Min Temperature Data'] = []
        outdata['Max Temperature Data'] = []
        outdata['Mean Temperature Data'] = []
        
        outdata['Min Wind Direction Data'] = []
        outdata['Max Wind Direction Data'] = []
        outdata['Mean Wind Direction Data'] = []
        
        outdata['Min Wind Speed Data'] = []
        outdata['Max Wind Speed Data'] = []
        outdata['Mean Wind Speed Data'] = []
        for item in features:
            for i in range(0, len(item)):
                value = item[i]
                if i == 0:
                    outdata['Min Temperature Data'].append(polyOut(value, "Min Temperature"))                          
                if i == 1:
                    outdata['Max Temperature Data'].append(polyOut(value, "Max Temperature"))
                if i == 2:
                    outdata['Mean Temperature Data'].append(polyOut(value, "Mean Temperature"))
                if i == 3:
                    outdata['Min Wind Direction Data'].append(polyOut(value, "Min Wind Direction"))
                if i == 4:
                    outdata['Max Wind Direction Data'].append(polyOut(value, "Max Wind Direction"))
                if i == 5:
                    outdata['Mean Wind Direction Data'].append(polyOut(value, "Mean Wind Direction"))
                if i == 6:
                    outdata['Min Wind Speed Data'].append(polyOut(value, "Min Wind Speed"))
                if i == 7:
                    outdata['Max Wind Speed Data'].append(polyOut(value, "Max Wind Speed"))
                if i == 8:
                    outdata['Mean Wind Speed Data'].append(polyOut(value, "Mean Wind Speed"))
                    
    #prepare point output
    if point:
        #append the inital queried geometry
        outdata['features'] = []
        outdata['features'].append({
            "geometry": (features[0][1][1], features[0][1][2])
        })

        outdata['Temperature Data'] = []
        outdata['Wind Direction Data'] = []
        outdata['Wind Speed Data'] = []
        for item in features:
            for key in item.keys():
                if 'Temperature Data' in item[key][4]:
                    outdata['Temperature Data'].append(pointOut("Temperature Observation", item[key]))
                if 'Wind Direction Data' in item[key][4]:
                    outdata['Wind Direction Data'].append(pointOut("Wind Direction Observation", item[key]))
                if 'Wind Speed Data' in item[key][4]:
                    outdata['Wind Speed Data'].append(pointOut("Wind Speed Observation", item[key]))
    
    #write the ouput
    with open('outGeoJSON2.geojson', 'w') as outfile:
        json.dump(outdata, outfile, indent=2)
                    
if __name__ == '__main__':
    start_time = time.time()
    
    # works with any arbitrary number of gribs BUT ONLY 1 GEOJSON
    # exmaple calls
    """
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "B:/Canada-d8-100m/Canada-d8-100m.geojson"

IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "C:/Users/Tom/Downloads/canada_provinces/canada_provinces/canada_provinces.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2"]" "B:/PointTest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P006.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P000.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/006/CMC_glb_WIND_TGL_10_latlon.15x.15_2020052500_P006.grib2"]" "B:/linetest.geojson"


IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/PolyTest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/linetest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2"]" "B:/linetest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/multilinetest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/PointTest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/MultiPointTest.geojson"
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P003.grib2", "B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/003/CMC_glb_WDIR_TGL_10_latlon.15x.15_2020052500_P003.grib2"]" "B:/MultiPointTest.geojson"
    
IP_Prototype.py "["B:/dd.weather.gc.ca/model_gem_global/15km/grib2/lat_lon/00/000/CMC_glb_TMP_TGL_2_latlon.15x.15_2020052500_P000_Copy.grib2"]" "B:/proj_test.geojson"

    """
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
    remove timing before final commit and submission to Tom K
    remove os if in memory reprojection gets working
    still to work on - in memory reprojection
    """
    

