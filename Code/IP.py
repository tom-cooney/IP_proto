from datetime import datetime
import json
import logging
import os
import re
import sys
import tempfile

from elasticsearch import Elasticsearch, exceptions
import numpy as np
from osgeo import gdal, osr
from pyproj import Transformer
import rasterio
import rasterio.mask
from rasterio.io import MemoryFile

LOGGER = logging.getLogger(__name__)

ES_INDEX = 'hackathon-lp'
DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

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

def get_files(layers, fh, mr):

    """
    ES search to find files names
    param layers : arrays of three layers
    param fh : forcast hour datetime
    param mr : model run
    return : files : arrays of threee file paths
    """
    # TODO: use env variable for ES connection

    es = Elasticsearch(['localhost:9200'])
    list_files = []
    weather_variables = []

    for layer in layers:
        for time_ in fh.split(','):    
            files = {}
            s_object = {
                'query': {
                    'bool': {
                        'must': {
                            'match': {'properties.layer.raw': layer}
                        },
                        'filter': [
                            {'term': {'properties.forecast_hour_datetime': time_}},
                            {'term': {'properties.reference_datetime': mr}}
                        ]
                    }
                }
            }

            try:
                res = es.search(index=ES_INDEX, body=s_object)

                try:
                    filepath =  res['hits']['hits'][0]['_source']['properties']['filepath']
                    fh_ = res['hits']['hits'][0]['_source']['properties']['forecast_hour_datetime']
                    mr = res['hits']['hits'][0]['_source']['properties']['reference_datetime']

                    files['filepath'] = filepath
                    files['forecast_hour'] = fh_
                    files['model_run'] = mr

                    list_files.append(files)

                except IndexError as error:
                    msg = 'invalid input value: {}' .format(error)
                    LOGGER.error(msg)
                    return None, None

            except exceptions.ElasticsearchException as error:
                msg = 'ES search failed: {}' .format(error)
                LOGGER.error(msg)
                return None, None
    return list_files


def reproject(x, y, inputSRS_wkt, raster_path):
    #reproject the point
    srs = osr.SpatialReference()
    srs.ImportFromWkt(inputSRS_wkt)
    #assume all geojson inputs have crs epsg:4326
    transformer = Transformer.from_crs("epsg:4326", srs.ExportToProj4())
    _x, _y = transformer.transform(x, y)
    
    ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
    geotransform = ds.GetGeoTransform()
    origin_x = geotransform[0]
    origin_y = geotransform[3]
    width = geotransform[1]
    height = geotransform[5]
    final_x = (_x - origin_x) / width
    final_y = (_y - origin_y) / height
    
    return [final_x, final_y]

def reproject_line(geojson_path, raster_path):
    to_return = []
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

            to_return.append(reproject(x, y, inputSRS_wkt, raster_path))
    print(to_return)
    return to_return

def reproject_poly(geojson_path, raster_path):
    ret = []
    to_return = []
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

                ret.append(reproject(x, y, inputSRS_wkt, raster_path))
    to_return.append(ret)
    return to_return
    
#get the value at a specific input point in a point query
def get_point(raster_list, geoJSON_path):
    to_return = {}
    i = 0
    
    #get inital lat/long coordinates
    with open(geoJSON_path) as f:
        gj = json.load(f)
    init_coords = gj['features'][0]['geometry']['coordinates']
    
    #reproject
    coords = reproject_line(geoJSON_path, raster_list[0])
    
    for raster_path in raster_list:
        #check if the input geojson crs matches the raster crs and reproject if needed and get input point geometry
        
        #setup and initialize vars for differentiating between temp, wdir, wspeed queries
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"
        
        x = int(coords[0][0])
        y = int(coords[0][1])

        ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
        band = ds.GetRasterBand(1)
        arr = band.ReadAsArray()
        to_return[i] = [init_coords[0][0], init_coords[0][1], arr[y][x], data_type]
        i += 1
        
    return to_return


#get the values along an input line in a line query
def get_line(raster_list, geoJSON_path):
    to_return = {}
    i = 0
    with open(geoJSON_path) as f:
        gj = json.load(f)
    input_line = gj['features'][0]['geometry']['coordinates']
    input_line = str(input_line)
    input_line = input_line.replace(" ", "")
    
    shapes = []
    shapes.append({
    'type': 'LineString',
    'coordinates': reproject_line(geoJSON_path, raster_list[0])
    })
    
    for raster_path in raster_list:        
        #setup and initialize vars for differentiating between temp, wdir, wspeed queries
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"
    
        #Open Raster
        with rasterio.open(raster_path) as src:
            #check if the input geojson crs matches the raster crs and reproject if needed
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
                    ds = ds[ds!=9999.0]
                    to_return[i] = [ds, data_type, input_line]
                    i += 1
        
    return to_return

#get the summary statistics for an input polygon in polygon queries    
def summ_stats_poly(raster_list, geoJSON_path):
    #setup returns
    to_return = {}
    i = 0
    
    shapes = []
    shapes.append({
        'type': 'Polygon',
        'coordinates': reproject_poly(geoJSON_path, raster_list[0])
    })
    
    for raster_path in raster_list:
        if "TMP" in raster_path:
            data_type = "Temperature Data"
        if "WDIR" in raster_path:
            data_type = "Wind Direction Data"
        if "WIND" in raster_path:
            data_type = "Wind Speed Data"
        
        #Open Raster
        with rasterio.open(raster_path) as src:
            #check if the input geojson crs matches the raster crs and reproject if needed
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
                    ds = ds[ds!=9999.0]
                    #create summary stats
                    min_val = np.min(ds, axis = None)
                    max_val = np.max(ds, axis = None)
                    mean_val = np.mean(ds, axis = None)
                    to_return[i] = [min_val, max_val, mean_val, data_type]
                    i += 1
    return to_return
        
def poly_out(string_name, forecast_hour, value):
    to_return = []
    to_return.append({
        'Forecast Hour': forecast_hour,
        string_name: value
    })
    return to_return

def point_out(string_name, forecast_hour, key):
    to_return = []
    to_return.append({
        "Forecast Hour": forecast_hour,
        string_name: key
            })
    return to_return

def write_output(features, forecast_hours, poly, line, point):
    #initalize dictionary to return and generic metadata
    i = 0
    
    #prepare line ouput
    if line and not poly and not point:
        #append the inital queried geometry
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": features[0][1][2]
        })

        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []
        
        
        for item in features:
            for key in item.keys():
                if "Temperature Data" in item[key][1]:
                    OUTDATA['Temperature Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Temperature Observation': item[key][0].tolist()
                    })
                if "Wind Direction Data" in item[key][1]:
                    OUTDATA['Wind Direction Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Wind Direction Observation': item[key][0].tolist()
                    })
                if "Wind Speed Data" in item[key][1]:
                    OUTDATA['Wind Speed Data'].append({
                        "Forecast Hour": forecast_hours[i],
                        'Wind Speed Observation': item[key][0].tolist()
                    })
                i += 1
                    
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
            for key in item.keys():
                if 'Temperature Data' in item[key][3]:
                    OUTDATA['Min Temperature Data'].append(poly_out("Min Temperature", forecast_hours[i], item[key][0]))
                    OUTDATA['Max Temperature Data'].append(poly_out("Max Temperature", forecast_hours[i], item[key][1]))
                    OUTDATA['Mean Temperature Data'].append(poly_out("Mean Temperature", forecast_hours[i], item[key][2]))
                if 'Wind Direction Data' in item[key][3]:
                    OUTDATA['Min Wind Direction Data'].append(poly_out("Min Wind Direction", forecast_hours[i], item[key][0]))
                    OUTDATA['Max Wind Direction Data'].append(poly_out("Max Wind Direction", forecast_hours[i], item[key][1]))
                    OUTDATA['Mean Wind Direction Data'].append(poly_out("Mean Wind Direction", forecast_hours[i], item[key][2]))
                if 'Wind Speed Data' in item[key][3]:
                    OUTDATA['Min Wind Speed Data'].append(poly_out("Min Wind Speed", forecast_hours[i], item[key][0]))
                    OUTDATA['Max Wind Speed Data'].append(poly_out("Miax Wind Speed", forecast_hours[i], item[key][1]))
                    OUTDATA['Mean Wind Speed Data'].append(poly_out("Mean Wind Speed", forecast_hours[i], item[key][2]))    
                i += 1
                    
    #prepare point output
    if point:
        #append the inital queried geometry
        OUTDATA['features'] = []
        OUTDATA['features'].append({
            "geometry": (features[0][1][0], features[0][1][1])
        })

        OUTDATA['Temperature Data'] = []
        OUTDATA['Wind Direction Data'] = []
        OUTDATA['Wind Speed Data'] = []
        for item in features:
            for key in item.keys():
                if i >= len(item.keys()):
                    break
                if 'Temperature Data' in item[key][3]:
                    OUTDATA['Temperature Data'].append(point_out("Temperature Observation", forecast_hours[i], item[key][2]))
                if 'Wind Direction Data' in item[key][3]:
                    OUTDATA['Wind Direction Data'].append(point_out("Wind Direction Observation", forecast_hours[i], item[key][2]))
                if 'Wind Speed Data' in item[key][3]:
                    OUTDATA['Wind Speed Data'].append(point_out("Wind Speed Observation", forecast_hours[i], item[key][2]))
                i += 1
    
    #write the ouput
    with open('outGeoJSON2.geojson', 'w') as outfile:
        json.dump(OUTDATA, outfile, indent=2)

if __name__ == '__main__':
    
    if len(sys.argv) < 4:
        print('Usage: %s <model> <forecast hours> <model run>' % sys.argv[0])
        sys.exit(1)
        
    model = sys.argv[1]
    fh = sys.argv[2]
    mr = sys.argv[3]

    var_list = ['TT', 'WD', 'WSPD']
    layers = []

    if model.upper() == 'HRDPS':
        model = 'HRDPS.CONTINENTAL_{}'

    for layer in var_list:
        layers.append(model.format(layer)) 

    result = get_files(layers, fh, mr)
    
    raster_list = []
    forecast_hours = []
    for element in result:
        raster_list.append(element["filepath"])
        forecast_hours.append(element["forecast_hour"])
    #get polygon/line/point to clip
    geoJSON_path = sys.argv[4]
    
    #setup variables for differentiating between point, line, polygon calls and other needed vars
    features = []
    poly = False
    line = False
    point = False
    
    #determine the query type between point, line, polygon and call the appropriate function 
    with open(geoJSON_path) as f:
        indata = json.load(f)
    for feature in indata['features']:
        if feature['geometry']['type'] == "Polygon" or feature['geometry']['type'] == "MultiPolygon":
            poly = True
            features.append(summ_stats_poly(raster_list, geoJSON_path))
            break
        elif feature['geometry']['type'] == "LineString" or feature['geometry']['type'] == "MultiLineString":
            line = True
            features.append(get_line(raster_list, geoJSON_path))
            break
        elif feature['geometry']['type'] == "Point" or feature['geometry']['type'] == "MultiPoint":
            point = True
            features.append(get_point(raster_list, geoJSON_path))
    
    #after putting necessary data in features call the writing output file function
    write_output(features, forecast_hours, poly, line, point)

