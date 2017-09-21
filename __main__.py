#!/usr/bin/env spark-submit
#

from __future__ import print_function
from os.path import expanduser, join, abspath

#import time
import json
import logging
import string
import random

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
hive = HiveContext(sc)


class DataFrameEngineUtils():

    @staticmethod
    def get_filtered_dataframe(dataframe,filters):
        print("filtering dataframe ")
        for filter_item in filters:
            count = str(dataframe.count())
            print("rows before filter: "+count)
            exp = filter_item.get("expression")
            print("\tfilter expression: "+exp)
            dataframe = dataframe.filter(exp)
            count = str(dataframe.count())
            print("rows after filter: "+count)
        return dataframe

    @staticmethod
    def register_inputs_as_tables(inputs):
        for input_item in inputs:
            name = input_item.get("name")
            data = input_item.get("data")
            print("Registering temp table name: "+name)
            data.registerTempTable(name)

    @staticmethod
    def drop_temp_tables(inputs):
        for input_item in inputs:
            name = input_item.get("name")
            print("Droping temp table name: "+name)
            try:
                hive.dropTempTable(name)
            except:
                print("the table: "+name+" doesn't exists, probably it was overwritten by some input")
    
    @staticmethod
    def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def persist_dataframe(name,method,dataframe):
        print("table: "+name+" will be persisted in hive")
        id = DataFrameEngineUtils.id_generator()
        print("Temporary table: "+name+"_"+id+" created")
        if method == "REPLACE":
            dataframe.registerTempTable(name+"_"+id)
            DataFrameEngineUtils.execute_query("drop table if exists "+name)
            DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
        elif method == "APPEND":
            try:
                hive.table(name)
                print("Table: "+name+" already exists, appending data")
                tmpdf = DataFrameEngineUtils.execute_query("select * from "+name)
                dataframe = tmpdf.unionAll(dataframe)
                dataframe.registerTempTable(name+"_"+id)
                DataFrameEngineUtils.execute_query("drop table if exists "+name)
                DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
            except Exception as inst:
                print(type(inst))
                print(inst.args)
                print(inst)
                print("Table: "+name+" don't exist, creating table with data")
                DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
        else:
            print("persist method not supported")
        hive.dropTempTable(name+"_"+id)
        print("Temporary table: "+name+"_"+id+" droped")

    @staticmethod
    def execute_query(query):
        print("Executing query: "+query)
        dataframe = hive.sql(query)
        count = str(dataframe.count())
        print("Query returned "+count+" records!")
        return dataframe

    @staticmethod
    def get_table_columns(table):
        print("Returning columns for table: "+table)
        return hive.table(table).columns

class InputEngineUtils():

    @staticmethod
    def import_loader(json_file):
        template_vars = {}
        if "import" in json_file:
            import_stage = json_file.get("import")
        if "template_vars" in json_file:
            template_vars = json_file.get("template_vars")
        with open(import_stage) as f_in:
            json_file = json.load(f_in)
        json_file = InputEngineUtils.process_template_vars(json_file,template_vars)
        return {"config":json_file,"template_vars":template_vars}

    @staticmethod
    def process_template_vars(json_data,template_vars):
        #print("Processing template variables: "+ json.dumps(template_vars))
        data = json.dumps(json_data)
        for var in template_vars:
            val = template_vars.get(var)
            var = "${"+var+"}"
            print("\tVariable: "+var+" Value: "+val)
            data = data.replace(var,val)
        json_data = json.loads(data)
        #print("Result: "+json.dumps(json_data))
        return json_data

    @staticmethod
    def get_input(input_item):
        source = input_item.get("source")
        print("Getting table from hive: "+source)
        filters = input_item.get("filters")
        query = "select * from "+source
        dataframe = DataFrameEngineUtils.execute_query(query)
        dataframe = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)
        return dataframe

    @staticmethod
    def get_inputs(inputs):
        print("building inputs ")
        inputs_result = []
        for input_item in inputs:
            input_df = InputEngineUtils.get_input(input_item)
            destination = input_item.get("destination")
            print("Creating input dataframe, name: "+destination)
            inputs_result.append({"name": destination, "data": input_df})
        return inputs_result
    
    @staticmethod
    def process_outputs(outputs,dataframe):
        print("building outputs ")
        output_result = []
        for output_item in outputs:
            table = output_item.get("table")
            filters = output_item.get("filters")
            dataframe_tmp = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)
            persist = output_item.get("persist")
            if persist == "TRUE":
                persist_method = output_item.get("persist_method")
                DataFrameEngineUtils.persist_dataframe(table,persist_method,dataframe_tmp)
            output ={
                "name": table, 
                "data": dataframe_tmp
            }
            print("Creating output dataframe, name: "+table)
            output_result.append(output)
        return output_result


class MergeStep():
    def __init__(self, step):
        self.step = step
        self.destination_columns = self.step.config.get("destination_columns")
        self.source_tables = self.step.config.get("source_tables")

    def build_table_query(self,table,columns):
        first = True
        query_cols = ""
        for column in self.destination_columns:
            column_name = columns.get(column)
            if first:
                query_cols = table+".`"+column_name+"` `"+column+"`"
                first = False
            else:
                query_cols = query_cols + ","+table+".`"+column_name+"` `"+column+"`"
        query = "select "+query_cols+" from "+table
        return query


    def execute(self):
        first = True
        for table_item in self.source_tables:
            table = table_item.get("table")
            columns = table_item.get("columns")
            query = self.build_table_query(table,columns)
            if first:
                dataframe = DataFrameEngineUtils.execute_query(query)
                first = False
            else:
                dftmp = DataFrameEngineUtils.execute_query(query)
                dataframe = dataframe.unionAll(dftmp)
        return dataframe



class JoinStep():
    def __init__(self, step):
        self.step = step
        self.source_table = self.step.config.get("source_table")
        self.destination_table = self.step.config.get("destination_table")
        self.type = self.step.config.get("join_type")
        self.ids = self.step.config.get("join")
        print("Join type: "+self.type+" initialized!")

    def columns_query_builder(self,table):
        first_val = True
        query_cols = ""
        for col in DataFrameEngineUtils.get_table_columns(table):
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
        join_query = self.join_query_builder()
        dataframe = DataFrameEngineUtils.execute_query(join_query)
        return dataframe

class Step():
    def __init__(self, stage, config):
        import_map = InputEngineUtils.import_loader(config)
        self.config = import_map.get("config")
        self.config = InputEngineUtils.process_template_vars(self.config,stage.template_vars)
        self.stage = stage
        self.type = self.config.get("type")
        self.inputs = InputEngineUtils.get_inputs(self.config.get("inputs"))
        self.outputs = self.config.get("outputs")
        print("Step type: "+self.type+" initialized!")

    def execute(self):
        print("Executing Step...")
        outputs_list = []
        DataFrameEngineUtils.register_inputs_as_tables(self.inputs)
        step = False
        if self.type == "join":
            step = JoinStep(self)
            outputdf = step.execute()
            outputs_list = InputEngineUtils.process_outputs(self.outputs,outputdf)
        if self.type == "merge":
            step = MergeStep(self)
            outputdf = step.execute()
            outputs_list = InputEngineUtils.process_outputs(self.outputs,outputdf)
        for output in outputs_list:
            self.stage.inputs.append(output)
        DataFrameEngineUtils.drop_temp_tables(self.inputs)
        

class Stage():
    def __init__(self, process, config):
        import_map = InputEngineUtils.import_loader(config)
        self.config = import_map.get("config")
        self.template_vars = import_map.get("template_vars")
        print(json.dumps(self.config))
        self.process = process
        self.name = self.config.get("stage_name")
        self.inputs = InputEngineUtils.get_inputs(self.config.get("inputs"))
        self.steps = self.config.get("steps")
        print("Stage: "+self.name+" initialized!")
    
    def execute(self):
        print("Executing Steps...")
        for step_config in self.steps:
            step = Step(self,step_config)
            DataFrameEngineUtils.register_inputs_as_tables(self.inputs)
            step.execute()
            DataFrameEngineUtils.register_inputs_as_tables(self.inputs)
        DataFrameEngineUtils.drop_temp_tables(self.inputs)

class Process():
    def __init__(self, config):
        self.name = config.get("process_name")
        self.stages = config.get("stages")
        self.hive_database = config.get("hive_database")
        DataFrameEngineUtils.execute_query("use "+self.hive_database)
        print("Process: "+self.name+" initialized!")
    
    def execute(self):
        print("Executing stages...")
        for stage_config in self.stages:
            stage = Stage(self,stage_config)
            stage.execute()
        
if __name__ == "__main__":
    with open("conf.json") as f_in:
        process_config = json.load(f_in)
    process = Process(process_config)
    process.execute()