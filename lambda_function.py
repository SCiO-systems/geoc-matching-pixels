import numpy as np
import numpy.ma as ma
import gdal
import os
import sys
import pathlib
import boto3
import json
import copy
import random
import string as st

# import logging
from botocore.exceptions import ClientError

s3 = boto3.client('s3')


def lambda_handler(event, context):
    
    body = json.loads(event['body'])
    json_file = body
    # json_file = event['body']

    path_to_save_temp_files = "/tmp/"

    # get  input json and extract geojson
    try:
        datasets = json_file["datasets"]
        target_area = json_file["target"]
    except Exception as e:
        print(e)

    if bool(target_area) == False:
        raise Exception("Empty target area")

    datasets_used = []
    for dataset in datasets:
        if dataset["chosen"] == True:
            datasets_used.append(dataset)

    gdal_warp_kwargs_target_area = {
        'format': 'GTiff',
        'cutlineDSName': json.dumps(target_area),
        'cropToCutline': True,
        #     'srcNodata' : 255,
        #     'dstNodata' : -9999,
        # 'creationOptions': ['COMPRESS=DEFLATE']
        
    }


    # get area data from s3 based on geojson and files that can be processed with mean function
    for dataset in datasets_used:
        # create the paths used
        s3_file_path = '/vsis3/geoc-slm-function-data/' + dataset["filename"]
        target_save_temp_file_path = path_to_save_temp_files + "target_" + dataset["filename"]

        # execute the data transfer and save resuls
        gdal.Warp(target_save_temp_file_path, s3_file_path, **gdal_warp_kwargs_target_area)

    files_list = os.listdir(path_to_save_temp_files)
    target_datafiles = [x for x in files_list if x.startswith("target")]

    # read a tiff in order to find dimension for the creation of the mul array
    f = gdal.Open(path_to_save_temp_files + target_datafiles[0])
    # mult _array is used for efficient mult between the resulting arrays in order not to keep the all in memory
    mult_array = np.ones((f.RasterYSize, f.RasterXSize))

    for dataset in datasets_used:
        # read one by one the tiffs and create their masked array representation
        file_array = gdal.Open(path_to_save_temp_files + "target_" + dataset["filename"]).ReadAsArray()
        file_array = ma.array(file_array, mask=np.logical_or(file_array <= -9999, file_array == 255), fill_value=-9999)

        # examine mean or majority
        if dataset["type"] == "numerical":
            #         print("mean")
            # create binary array, 0 if not inside range and 1 elsewhere, no data are kept as no data
            masked_array = ma.where(
                np.logical_and(file_array >= dataset["thresholds"][0], file_array <= dataset["thresholds"][1]), 1, 0)
        else:
            #         print("majority")
            # keep mask because np.isin transforms the masked array to a simple numpy array
            mask = ma.getmask(file_array)
            # check which elements if the array are inside the majority list
            masked_array = ma.array(np.isin(file_array, dataset["classes"]), mask=mask)

        # multiply the product of previous masked arrays with the new
        # if at least one pixel is zero in any masked array then this pixel in the final map will be zero
        mult_array = ma.multiply(masked_array, mult_array)
    mult_array = mult_array.astype(np.int16)
    mult_array.set_fill_value(-9999)

    # output_tif_path : path to output file including its title in string format.
    # array_to_save : numpy array to be saved, 3d shape with the following format (no_bands, width, height). If only one band then should be extended with np.expand_dims to the format (1, width, height).
    # reference_tif : path to tif which will be used as reference for the geospatial information applied to the new tif.

    def save_arrays_to_tif(output_tif_path, array_to_save, reference_tif):

        if len(array_to_save.shape) == 2:
            array_to_save = np.expand_dims(array_to_save, axis=0)

        no_bands, width, height = array_to_save.shape

        old_raster_used_for_projection = gdal.Open(reference_tif)

        # width = old_raster_used_for_projection.RasterYSize
        # height = old_raster_used_for_projection.RasterXSize
        gt = old_raster_used_for_projection.GetGeoTransform()
        wkt_projection = old_raster_used_for_projection.GetProjectionRef()

        driver = gdal.GetDriverByName("GTiff")
        DataSet = driver.Create(output_tif_path, height, width, no_bands, gdal.GDT_Int16)

        # geo_info = [gt,wkt_projection]
        # for covering the area of the reference tif
        DataSet.SetGeoTransform(gt)

        # for wgs84 covering the whole world
        #     geo_trans = (-180.0,360.0/height,0.0,90,0.0,-180/width)
        #     DataSet.SetGeoTransform(geo_trans)
        DataSet.SetProjection(wkt_projection)

        # no data value
        ndval = -9999
        for i, image in enumerate(array_to_save, 1):
            DataSet.GetRasterBand(i).WriteArray(image)
            DataSet.GetRasterBand(i).SetNoDataValue(ndval)
        DataSet = None
        #         print(output_tif_path, " has been saved")
        return


    random_save_filename = ''.join(random.choices(st.ascii_lowercase + st.digits, k=10)) + ".tif"

    # masked array must be filled, select carefully the reference tif
    save_arrays_to_tif(path_to_save_temp_files + random_save_filename, mult_array.filled(),
                       path_to_save_temp_files + target_datafiles[0])

    #     s3 = boto3.resource('s3')
    # for bucket in s3.buckets.all():
    #     print(bucket.name)

    # for file in my_bucket.objects.all():
    #     print(file.key)

    path_to_file_for_upload = path_to_save_temp_files + random_save_filename
    target_bucket = "geoc-temp"

    string = path_to_file_for_upload.split("/")
    object_name = string[-1]

    # if (len(sys.argv)==4):
    #     object_name = sys.argv[3]

    # Upload the file
    #     s3 = boto3.client('s3')
    try:
        response = s3.upload_file(path_to_file_for_upload, target_bucket, object_name)
    #         print("Uploaded file: " + string[-1])
    except ClientError as e:
        logging.error(e)

    my_output = {
        "URL": "https://geoc-temp.s3.eu-central-1.amazonaws.com/" + random_save_filename
    }

    return {
        "statusCode": 200,
        "body": json.dumps(my_output)
    }

