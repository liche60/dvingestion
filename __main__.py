#!/usr/bin/env spark-submit
#

from __future__ import print_function
from os.path import expanduser, join, abspath

#import time
import json
import logging

#from impala.dbapi import connect
#from impala.util import as_pandas
from pyspark.context import SparkContext
from pyspark.sql import HiveContext, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
"""

"""
sc =SparkContext()
sc.setLogLevel("OFF")
sql = SQLContext(sc)

experto = (sql.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load("experto.csv"))
cdav = (sql.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .load("cdav.csv"))

sql.registerDataFrameAsTable(experto, "experto")
sql.registerDataFrameAsTable(cdav, "cdav")

def columns_query_builder(table):
    first_val = True
    query_cols = ""
    for col in sql.table(table).columns:
        if first_val:
            query_cols = table+"."+col+" "+table+"_"+col
            first_val = False
        else:
            query_cols = query_cols + ","+table+"."+col+" "+ table+"_"+col
    return query_cols

def join_query_builder(source_table,destination_table,ids,join_type):
    query_cols = columns_query_builder(source_table) + "," + columns_query_builder(destination_table)
    join_query = "select "+query_cols+" from "+source_table+" "+join_type+" "+destination_table+" ON "
    first_val = True
    for col in ids:
        if first_val:
            first_val = False
            join_query = join_query + " "+source_table+"."+col.get("source")+" = "+destination_table+"."+col.get("destination")
        else:
            join_query = join_query + " AND "+source_table+"."+col.get("source")+" = "+destination_table+"."+col.get("destination")
    return join_query

def join_executer(process,stage,join):
    dbname = process.get("database_name")
    source_table = join.get("source_table")
    destination_table = join.get("destination_table")
    ids = join.get("join")
    join_query = join_query_builder(source_table,destination_table,ids,"FULL OUTER JOIN")
    outputdf = sql.sql(join_query)
    outputdf.show()


    """
    Try:
        firstdf.join(
            seconddf, 
            [col(f) == col(s) for (f, s) in zip(columnsFirstDf, columnsSecondDf)], 
            "inner"
        )
    """

def step_handler(process,stage,step):
    step_type = step.get("type")
    if step_type == "join":
        join_executer(process,stage,step)
    else:
        print("step type not supported")

def stage_executer(process,stage):
    for step in stage.get("steps"):
        step_handler(process,stage,step)

if __name__ == "__main__":
    #logging.getLogger("py4j").setLevel(logging.ERROR)
    with open("conf.json") as f_in:
        data = json.load(f_in)
    for stage in data.get("stages"):
        stage_executer(data,stage)
    

    
    #rows = baby_names.map(lambda line: line.split(","))
    #for row in rows.take(rows.count()): print(row[2])
    #sqlContext = HiveContext(sc)
    #sqlContext.sql("use pruebas_cuadre")
    #sqlContext.sql("FROM p17 SELECT count(*)").show()
