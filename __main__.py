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
from pyspark.sql import SQLContext,HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
"""

"""
sc =SparkContext()
sc.setLogLevel("OFF")
sql = SQLContext(sc)
hive = HiveContext(sc)

class DataFrameEngineUtils():

    @staticmethod
    def get_filtered_dataframe(dataframe,filters):
        for filter_item in filters:
            exp = filter_item.get("expresion")
            dataframe = dataframe.filter(exp)
        return dataframe

    @staticmethod
    def register_inputs_as_tables(inputs):
        for input_item in inputs:
            name = input_item.get("name")
            data = input_item.get("data")
            sql.registerDataFrameAsTable(name,data)

    @staticmethod
    def drop_temp_tables(inputs):
        for input_item in inputs:
            name = input_item.get("name")
            sql.dropTempTable(name)

    @staticmethod
    def execute_hive_query(db,query):
        hive.sql("use "+db)
        dataframe = hive.sql(query)
        return dataframe
    
    @staticmethod
    def execute_mem_query(query):
        dataframe = sql.sql(query)
        return dataframe

    @staticmethod
    def get_mam_table_columns(table):
        return sql.table(table).columns

class InputEngineUtils():
    
    @staticmethod
    def get_hive_input(input_item):
        hive_db = input_item.get("hive_db")
        hive_table = input_item.get("hive_table")
        filters = input_item.get("filters")
        query = "select * from "+hive_table
        dataframe = DataFrameEngineUtils.execute_hive_query(hive_db,query)
        dataframe = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)
        return dataframe

    @staticmethod
    def get_mem_input(input_item):
        hive_db = input_item.get("hive_db")
        hive_table = input_item.get("hive_table")
        filters = input_item.get("filters")
        hive.sql("use "+hive_db)
        dataframe = hive.sql("select * from "+hive_table)
        dataframe = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)

    @staticmethod
    def get_input(input_item):
        in_type = input_item.get("type")
        if in_type == "hive":
            return InputEngineUtils.get_hive_input(input_item)
        elif in_type == "mem":
            return InputEngineUtils.get_mem_input(input_item)
        else:
            print("input type: "+in_type+" not supported")

    @staticmethod
    def get_inputs(inputs):
        inputs = []
        for input_item in inputs:
            input_df = InputEngineUtils.get_input(input_item)
            in_mem_table_name = input_item.get("in_mem_table_name")
            inputs.append({"name": in_mem_table_name, "data": input_df})
        return inputs


class JoinStep():
    def __init__(self, config, stage_inputs,inputs):
        self.stage_inputs = stage_inputs
        self.inputs = inputs
        self.source_table = config.get("source_table")
        self.destination_table = config.get("destination_table")
        self.type = config.get("join_type")
        self.ids = config.get("join")
        print("Join type: "+self.type+" initialized!")

    def columns_query_builder(self,table):
        first_val = True
        query_cols = ""
        for col in DataFrameEngineUtils.get_mam_table_columns(table):
            if first_val:
                query_cols = table+".`"+col+"` `"+table+"_"+col+"`"
                first_val = False
            else:
                query_cols = query_cols + ","+table+".`"+col+"` `"+ table+"_"+col+"`"
        return query_cols

    def join_query_builder(self):
        query_cols = self.columns_query_builder(self.source_table) + "," + self.columns_query_builder(self.destination_table)
        join_query = "select "+query_cols+" from "+self.source_table+" "+self.type+" "+self.destination_table+" ON "
        first_val = True
        for col in self.ids:
            if first_val:
                first_val = False
                join_query = join_query + " "+self.source_table+".`"+col.get("source")+"` = "+self.destination_table+".`"+col.get("destination")+"`"
            else:
                join_query = join_query + " AND "+self.source_table+".`"+col.get("source")+"` = "+self.destination_table+".`"+col.get("destination")+"`"
        return join_query

    def execute(self):
        DataFrameEngineUtils.register_inputs_as_tables(self.stage_inputs)
        DataFrameEngineUtils.register_inputs_as_tables(self.inputs)
        join_query = self.join_query_builder()
        print("Join Query: "+join_query)
        dataframe = DataFrameEngineUtils.execute_mem_query(join_query)
        dataframe.show()
        drop_temp_tables(self.stage_inputs)
        drop_temp_tables(self.inputs)

class Step():
    def __init__(self, config, stage_inputs):
        self.type = config.get("type")
        self.config = config
        self.stage_inputs = stage_inputs
        self.inputs = InputEngineUtils.get_inputs(config.get("inputs"))
        print("Step type: "+self.type+" initialized!")

    def execute(self):
        print("Executing Step...")
        if self.type == "Join"
            step = JoinStep(self.config,self.stage_inputs,self.inputs)
            step.execute()
        else:
            print("Step type: "+self.type+" not supported")

class Stage():
    def __init__(self, config):
        self.name = config.get("stage_name")
        self.inputs = InputEngineUtils.get_inputs(config.get("inputs"))
        self.steps = config.get("steps")
        print("Stage: "+self.name+" initialized!")
    
    def execute(self):
        print("Executing Steps...")
        for step_config in self.steps
            step = Step(step_config,self.inputs)
            step.execute()

class Process():
    def __init__(self, config):
        self.name = config.get("process_name")
        self.stages = config.get("stages")
        print("Process: "+self.name+" initialized!")
    
    def execute(self):
        print("Executing stages...")
        for stage_config in self.stages
            stage = Stage(stage_config)
            stage.execute()
        
if __name__ == "__main__":
    with open("conf.json") as f_in:
        process_config = json.load(f_in)
    process = Process(process_config)
    process.execute()