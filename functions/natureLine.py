from google.cloud import bigquery
import flask
import json
import pandas as pd
import geopandas as gpd
from shapely import wkt
from flask import jsonify
import oracledb
import os
import logging #package for error logging
from env import * # you must make an env.py file with your oracle.db connection named as "conn"


#tracks error messaging
logging.basicConfig(level=logging.INFO)

def run():
    # conn = oracledb.connect(
    #         the actual connection is found in env.py
    # )
    curs = conn.cursor()
    #old SQL for centerpoint using only SQL
#         sql = '''
# SELECT source_feature_id, 
# sdo_lrs.convert_to_std_geom(sdo_lrs.locate_pt(sdo_lrs.convert_to_lrs_geom(shape,3)
#     ,sdo_geom.sdo_length(shape,3)/2)).sdo_point.x as x,
# sdo_lrs.convert_to_std_geom(sdo_lrs.locate_pt(sdo_lrs.convert_to_lrs_geom(shape,3)
#     ,sdo_geom.sdo_length(shape,3)/2)).sdo_point.y as y
# FROM SOURCE_FEATURE_PRE_Line
# '''
    sql = '''
SELECT source_feature_id, SDO_UTIL.TO_WKTGEOMETRY(
     shape
     ) AS WKT
FROM SOURCE_FEATURE_PRE_Line
'''
    curs.execute(sql)
    columns = [col[0] for col in curs.description]
    curs.rowfactory = lambda *args: dict(zip(columns, args))
    out = curs.fetchall()

    df = pd.DataFrame(out)
    df['WKT'] = df['WKT'].astype(str)
    df['geometry'] = df.WKT.apply(wkt.loads)
    df.drop('WKT', axis=1, inplace=True) #Drop WKT column
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf['cent'] = gdf.representative_point()
    gdf["X"] = gdf.cent.x
    gdf["Y"] = gdf.cent.y
    gcp = gdf.drop(columns=['geometry', 'cent'])

    PROJECT_ID = 'ut-dnr-biobase-dev'
    client = bigquery.Client(project=PROJECT_ID, location="US")
    # set location 
    dataset_id = 'biobase'
    table_id = 'natureLine'
    # set config
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
          gcp, table_ref, job_config=job_config
    ) 
    job.result()  # Wait for the job to complete.
      
    curs.close()
    conn.close()
    
    return "Loaded {} rows into {}:{}".format(job.output_rows, dataset_id, table_id)
