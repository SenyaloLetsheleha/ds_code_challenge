# -*- coding: utf-8 -*-
"""

@author: Senyalo Ishmail Letsheleha - City of Cape Town - Data Science Unit Code Challenge
"""

import boto3
import json
import logging
import time
import geopandas as gpd
import pandas as pd
import numpy as np
import json
from shapely import geometry
import requests
from shapely import Point
import h3 as h3
import pytz


def download_data():
    startTime = time.time()
    logging.basicConfig(level=logging.INFO)
    # set the AWS access keys
    aws_access_key_id = 'AKIAYH57YDEWMHW2ESH2'
    aws_secret_access_key = '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'

    # set the S3 bucket name
    bucket_name = 'cct-ds-code-challenge-input-data'

    # create a client object for S3
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url='https://s3.af-south-1.amazonaws.com')

    # use the list_objects_v2 method to list the objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    #object key
    object_key = 'city-hex-polygons-8.geojson'

    #local filename
    local_filename = 'city-hex-polygons-8.geojson'

    # iterate over the contents of the bucket and print the object names
    for obj in response['Contents']:
        print(obj['Key'])


    #Check if file exists in the s3 bucket 
    file_path = 'city-hex-polygons-8.geojson'

    try:
        s3.head_object(Bucket=bucket_name, Key=file_path)
        print(f'The file {file_path} exists in the {bucket_name} bucket.')
    except:
        print(f'The file {file_path} does not exist in the {bucket_name} bucket.')


    #Using s3 get_object
    response = s3.get_object(Bucket=bucket_name, Key=object_key)


    content = response['Body'].read().decode('utf-8')

    #Write file locally
    with open(local_filename, 'w') as f:
        f.write(content)

    print(f"GeoJSON file '{object_key}' successfully downloaded to '{local_filename}'")


    endTime = time.time()

    #Logging the data 
    logging.info(f"Time taken to extract data section 1: {endTime - startTime:.2f} seconds.")
    
download_data()



def join_data():
    startTime = time.time()
    logging.basicConfig(level=logging.INFO)
    
    #Importing data and converting to a GeoDataFrame object
    sr = pd.read_csv('sr_hex.csv')
    sr_gdf = gpd.GeoDataFrame(
        sr, geometry=gpd.points_from_xy(sr.longitude, sr.latitude))
    sr_gdf = sr_gdf.set_crs('epsg:4326')


    # Making a copy of the dataframe
    sr_gdf_copy = sr_gdf.copy()


    # Setting index to 0 were latitude and longitude is nan
    sr_gdf_copy.loc[sr_gdf_copy['latitude'].isna() | sr_gdf_copy['longitude'].isna(), 'h3_level8_index'] = 0
    sr_gdf_copy = sr_gdf_copy.drop(['geometry'], axis=1)

    sr_gdf = sr_gdf.dropna(subset=['latitude', 'longitude'])


    sr_gdf['latitude'] = sr_gdf['latitude'].astype(float)
    sr_gdf['longitude'] = sr_gdf['longitude'].astype(float)


    #Reading in geojson file and performing a join
    hexagons_gdf = gpd.read_file('city-hex-polygons-8.geojson')

    merged_gdf = gpd.sjoin(sr_gdf, hexagons_gdf,how='left', op='intersects')
    merged_gdf = merged_gdf.rename(columns={'index': 'h3_level8_index'})
    merged_gdf = merged_gdf.drop(['geometry', 'index_right', 'centroid_lat','centroid_lon'], axis=1)

    #Defining failed joins where h3_level8_index is nan
    failed_joins = merged_gdf[merged_gdf['h3_level8_index'].isna()].shape[0] 

    logging.info(f'{failed_joins} records failed to join.')



    #Defining error threshold and conditional
    error_threshold = 212364
    if failed_joins > error_threshold:
        raise ValueError(f'Too many failed joins: {failed_joins}')

    #Appending dataframe and writing to csv
    merged_gdf = merged_gdf.append(sr_gdf_copy)
    merged_gdf.sort_index(ascending=True)
    merged_gdf = merged_gdf.drop(merged_gdf.columns[[0]],axis = 1)
    final_merged = merged_gdf[merged_gdf['h3_level8_index'].notna()]
    final_merged.to_csv('sr_hex.csv', index=False)
    endTime = time.time()

    #Logging the data 
    logging.info(f"Time taken for join section 2: {endTime - startTime:.2f} seconds.")


join_data()

def augment_data():
    
    startTime = time.time()
    logging.basicConfig(level=logging.INFO)
    #Importing data
    df = pd.read_csv('sr_hex.csv')

    #GeoDataFrame created by converting the latitude and longitude columns into shapely Point objects:
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')

    # Calculating the centroid of BELLVILLE SOUTH by selecting it from the GeoDataFrame and using the centroid attribute:
    suburb_name = 'BELLVILLE SOUTH'
    suburb = gdf[gdf['official_suburb'] == suburb_name]
    centroid = suburb.geometry.centroid.values[0]

    #Accessing co-ordinates of centroid 
    centroid_lat = centroid.y
    centroid_lon = centroid.x



    #DataFrame filtered to include only the rows within 1 minute of the centroid
    gdf_filtered = gdf[
        (abs(gdf['latitude'] - centroid_lat) <= 1/60) &
        (abs(gdf['longitude'] - centroid_lon) <= 1/60)
    ]

    #Drop geometry column
    gdf_filtered = gdf_filtered.drop(['geometry'], axis=1)




    #Download-Wind-data
    url = 'https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx?DocumentName=Wind_direction_and_speed_2020.ods&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2Fopendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods'
    response = requests.get(url)
    with open('Wind_direction_and_speed_2020.ods', 'wb') as f:
        f.write(response.content)



    # Need to install package odfpy (pip install odfpy) to open this file Wind_direction_and_speed_2020.ods>>  
    wind_data = pd.read_excel("Wind_direction_and_speed_2020.ods", engine="odf")


    #Prepare wind data
    wind_Bel = wind_data.iloc[:, [0,3,4]]
    wind_Bel = wind_Bel.rename(columns={wind_Bel.columns[0]: 'Date & Time', wind_Bel.columns[1]: 'Wind Dir V',
                             wind_Bel.columns[2]: 'Wind Speed V'})
    wind_Bel = wind_Bel.iloc[4:8788]




    #Extracting date and hour from gdf_filtered
    gdf_filtered["creation_timestamp"] = pd.to_datetime(gdf_filtered['creation_timestamp'])
    gdf_filtered['date'] = gdf_filtered['creation_timestamp'].dt.date
    gdf_filtered['hour'] = gdf_filtered['creation_timestamp'].dt.hour

    #Extracting date and hour from wind data
    wind_Bel['Date & Time'] = pd.to_datetime(wind_Bel['Date & Time'])
    wind_Bel['date'] = wind_Bel['Date & Time'].dt.date
    wind_Bel['hour'] = wind_Bel['Date & Time'].dt.hour


    # merge the DataFrames based on matching date and hour
    merged_df = pd.merge(wind_Bel, gdf_filtered, on=['date', 'hour'], how='right')
    merged_df = merged_df.sort_values(by='Date & Time')
    merged_df = merged_df.drop(['Date & Time', 'date', 'hour'], axis=1)
    merged_df.to_csv('sr_hex_2.csv', index=False)
    endTime = time.time()

    #Logging the data 
    logging.info(f"Time taken to augment data section 5(1 and 2): {endTime - startTime:.2f} seconds.")

augment_data()

def anonymise_data():
    
    startTime = time.time()
    logging.basicConfig(level=logging.INFO)
    
    df = pd.read_csv('sr_hex_2.csv')


    df = df.drop(columns=['reference_number','directorate','department', 'branch', 'section', 'code_group', 'code','cause_code_group', 'cause_code', 'official_suburb'])

   

    df["creation_timestamp"] = pd.to_datetime(df['creation_timestamp'])
    df['creation_timestamp'] = df['creation_timestamp'].dt.tz_convert(pytz.UTC)
    df['creation_timestamp'] = df['creation_timestamp'].astype(np.int64) // 10**9

    df["completion_timestamp"] = pd.to_datetime(df['completion_timestamp'])
    df['completion_timestamp'] = df['completion_timestamp'].dt.tz_convert(pytz.UTC)
    df['completion_timestamp'] = df['completion_timestamp'].astype(np.int64) // 10**9

    df["creation_timestamp"] = df["creation_timestamp"].apply(lambda x: round(x / 21600) * 21600)
    df["completion_timestamp"] = df["completion_timestamp"].apply(lambda x: round(x / 21600) * 21600)

    #Used higher resolution of 11 to ensures that the location accuracy is preserved to within 500 m.
    df["lat_lon_anonymised"] = df.apply(lambda x: h3.geo_to_h3(x["latitude"], x["longitude"], 11), axis=1)

    df = df.drop(columns=["latitude", "longitude"])

    df.to_csv("sr_hex_2_anonymized.csv", index=False)

    
    endTime = time.time()

    #Logging the data 
    logging.info(f"Time taken to anonymise data section 5(3): {endTime - startTime:.2f} seconds.")

anonymise_data()





'''Tried the below code encountered  error 
ClientError: An error occurred (OverMaxRecordSize) 
when calling the SelectObjectContent operation: 
The character number in one record is more than our max threshold, maxCharsPerRecord: 1,048,576'''



# # set the SQL-like query to filter the data
# query = "SELECT * FROM S3Object s WHERE s.properties.resolution = '8'"

# #query = "SELECT s3object[*].properties.h3_8 FROM S3Object s3object"
# # set the input serialization format
# input_serialization = {'JSON': {'Type': 'DOCUMENT'}}

# # set the output serialization format
# output_serialization = {'JSON': {}}

# # use S3 SELECT to retrieve the filtered data
# response = s3.select_object_content(
#     Bucket=bucket_name,
#     Key=file_path,
#     ExpressionType='SQL',
#     Expression=query,
#     InputSerialization=input_serialization,
#     OutputSerialization=output_serialization
# )

# # create an empty list to store the filtered features
# filtered_features = []

# # iterate over the response records and append the filtered features to the list
# for event in response['Payload']:
#     if 'Records' in event:
#         records = event['Records']['Payload'].decode('utf-8')
#         features = json.loads(records)['features']
#         filtered_features += features

# # create a new GeoJSON object with the filtered features
# filtered_geojson = {'type': 'FeatureCollection', 'features': filtered_features}

# # save the filtered GeoJSON object as a new file locally
# with open('filtered_data.geojson', 'w') as f:
#     json.dump(filtered_geojson, f)

