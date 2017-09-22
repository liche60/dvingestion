#!/usr/bin/env spark-submit
#

from __future__ import print_function
from os.path import expanduser, join, abspath

#import time
import json
import logging
import string
import random
import sys

#from impala.dbapi import connect
#from impala.util import as_pandas
from pyspark.context import SparkContext
from pyspark.sql import SQLContext,HiveContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
"""

"""
import logging

sc =SparkContext()
sc.setLogLevel("OFF")
hive = HiveContext(sc)
STAGE_NAME = ""
STEP_NAME = ""

class Logger:

    def __init__(self, process_name):
        self.process_name = process_name
        self.log = self.setup_custom_logger()

    def setup_custom_logger(self):
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S')
        handler = logging.FileHandler('log.txt', mode='w')
        handler.setFormatter(formatter)
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        screen_handler.setFormatter(formatter)
        logger = logging.getLogger(self.process_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
        return logger

    def info(self,message):
        prefix = "["+self.process_name+"]"
        if STAGE_NAME != "":
            prefix = prefix + "["+STAGE_NAME+"]"
            if STEP_NAME != "":
                prefix = prefix + "["+STEP_NAME+"]"
        self.log.info(prefix+" "+message)

    def debug(self,message):
        prefix = "["+self.process_name+"]"
        if STAGE_NAME != "":
            prefix = prefix + "["+STAGE_NAME+"]"
            if STEP_NAME != "":
                prefix = prefix + "["+STEP_NAME+"]"
        self.log.debug(prefix+" "+message)
    
    def error(self,message):
        prefix = "["+self.process_name+"]"
        if STAGE_NAME != "":
            prefix = prefix + "["+STAGE_NAME+"]"
            if STEP_NAME != "":
                prefix = prefix + "["+STEP_NAME+"]"
        self.log.error(prefix+" "+message)

class DataFrameEngineUtils():

    @staticmethod
    def get_filtered_dataframe(dataframe,filters):
        LOGGER.debug(" *** Filtrando dataframe...")
        for filter_item in filters:
            count = str(dataframe.count())
            LOGGER.debug(" *** Registros antes del filtro: "+count)
            exp = filter_item.get("expression")
            LOGGER.debug("\t\t *** Filtro: "+exp)
            dataframe = dataframe.filter(exp)
            count = str(dataframe.count())
            LOGGER.debug(" *** Registros despues del filtro: "+count)
        return dataframe
    
    @staticmethod
    def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def persist_dataframe(name,method,dataframe):
        dataframe = dataframe.cache()
        LOGGER.debug("persist: La tabla "+name+" se guardara permanentemente en HIVE")
        countdf = str(dataframe.count())
        id = DataFrameEngineUtils.id_generator()
        if method == "REPLACE":
            LOGGER.debug("** persist.REPLACE: Creando tabla temporal: "+name+"_"+id+" con "+countdf+" registros")
            dataframe.registerTempTable(name+"_"+id)
            DataFrameEngineUtils.execute_query("drop table if exists "+name)
            LOGGER.debug("** persist.REPLACE: La tabla en HIVE "+name+" fue eliminada para ser recreada")
            DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
            hive.dropTempTable(name+"_"+id)
            LOGGER.debug("** persist.REPLACE: La tabla temporal "+name+"_"+id+" fue eliminada")
            newtable = hive.table(name)
            count = str(newtable.count())
            LOGGER.debug("** persist.REPLACE: La tabla "+name+" fue creada en HIVE con "+count+" registros")
        elif method == "APPEND":
            try:
                table = hive.table(name)
                table.cache()
                print("1===========")
                table.show()
                DataFrameEngineUtils.execute_query("drop table if exists "+name)
                df = dataframe.union(table)
            except:
                df = dataframe
            print("2===========")
            df.show()
            count = str(table.count())
            LOGGER.debug("La Tabla: "+name+" ya existe, y tiene "+count+" registros, se insertaran los registros nuevos!")
            LOGGER.debug("Creando tabla temporal: "+name+"_"+id+" con "+countdf+" registros que seran unidos con los "+count+" registros de la tabla actual")
            df.registerTempTable(name+"_"+id)
            DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
            hive.dropTempTable(name+"_"+id)
            hive.dropTempTable(name+"_"+id2)
            newtable = hive.table(name)
            newtable.show()
            count = str(newtable.count())
            LOGGER.debug("La tabla "+name+"fue creada en HIVE con "+count+" registros")
            LOGGER.debug("La tabla temporal "+name+"_"+id+" fue eliminada")
            #except Exception as inst:
                #LOGGER.debug("Ocurrio un error insertando los nuevos registros a la tabla: "+name+"_"+id+" se tratara de recrear la tabla")
                #LOGGER.debug("Creando tabla temporal: "+name+"_"+id+" con "+countdf+" registros")
                #dataframe.registerTempTable(name+"_"+id)
                #DataFrameEngineUtils.execute_query("create table "+name+" as select * from "+name+"_"+id)
                #hive.dropTempTable(name+"_"+id)
                #LOGGER.debug("La tabla temporal "+name+"_"+id+" fue eliminada")
        else:
            LOGGER.error("persist method not supported")

    @staticmethod
    def execute_query(query):
        LOGGER.debug(" *** Ejecutando query: "+query)
        dataframe = hive.sql(query)
        count = str(dataframe.count())
        LOGGER.debug("\t\t *** El Query retorno "+count+" registros!")
        return dataframe

    @staticmethod
    def get_table_columns(table):
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
            data = data.replace(var,val)
        json_data = json.loads(data)
        #print("Result: "+json.dumps(json_data))
        return json_data

    @staticmethod
    def get_input(input_item):
        source = input_item.get("source")
        filters = input_item.get("filters")
        query = "select * from "+source
        dataframe = DataFrameEngineUtils.execute_query(query)
        dataframe = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)
        return dataframe

    @staticmethod
    def get_inputs(inputs):
        inputs_result = []
        for input_item in inputs:
            input_df = InputEngineUtils.get_input(input_item)
            destination = input_item.get("destination")
            input_df.registerTempTable(destination)
        return inputs_result
    
    @staticmethod
    def process_outputs(outputs,dataframe):
        for output_item in outputs:
            table = output_item.get("table")
            filters = output_item.get("filters")
            dataframe_tmp = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)
            persist = output_item.get("persist")
            if persist == "TRUE":
                persist_method = output_item.get("persist_method")
                DataFrameEngineUtils.persist_dataframe(table,persist_method,dataframe_tmp)
            else:
                dataframe_tmp.registerTempTable(table)


class MergeStep():
    def __init__(self, step):
        LOGGER.info("Iniciando Merge...")
        self.step = step
        self.destination_columns = self.step.config.get("destination_columns")
        self.source_tables = self.step.config.get("source_tables")
        LOGGER.info("Merge Iniciado!")

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
        LOGGER.info("Ejecutando Merge...")
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
        LOGGER.info("Merge ejecutado!")
        return dataframe



class JoinStep():
    def __init__(self, step):
        LOGGER.info("Iniciando Join...")
        self.step = step
        self.source_table = self.step.config.get("source_table")
        self.destination_table = self.step.config.get("destination_table")
        self.type = self.step.config.get("join_type")
        self.ids = self.step.config.get("join")
        LOGGER.info("Join iniciado!")

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
        LOGGER.info("Ejecutando Join...")        
        join_query = self.join_query_builder()
        dataframe = DataFrameEngineUtils.execute_query(join_query)
        LOGGER.info("Join ejecutado!")        
        return dataframe

class Step():
    def __init__(self, stage, config):
        self.name = config.get("step_name")
        global STEP_NAME
        STEP_NAME = self.name
        LOGGER.info("Iniciando Step...")
        if "description" in config:
            self.description = config.get("description")
            LOGGER.info(self.description)
        import_map = InputEngineUtils.import_loader(config)
        self.config = import_map.get("config")
        self.config = InputEngineUtils.process_template_vars(self.config,stage.template_vars)
        self.config = InputEngineUtils.process_template_vars(self.config,stage.process.process_vars)
        self.stage = stage
        self.type = self.config.get("type")
        self.inputs = InputEngineUtils.get_inputs(self.config.get("inputs"))
        self.outputs = self.config.get("outputs")
        LOGGER.info("Step iniciado!")

    def execute(self):
        global STEP_NAME
        LOGGER.info("Ejecutando step...")
        step = False
        if self.type == "join":
            step = JoinStep(self)
            outputdf = step.execute()
            InputEngineUtils.process_outputs(self.outputs,outputdf)
        if self.type == "merge":
            step = MergeStep(self)
            outputdf = step.execute()
            InputEngineUtils.process_outputs(self.outputs,outputdf)
        LOGGER.info("Step finalizado")
        STEP_NAME = ""
        

class Stage():
    def __init__(self, process, config):
        global STAGE_NAME
        global STEP_NAME
        self.name = config.get("stage_name")
        STAGE_NAME = self.name
        STEP_NAME = ""
        LOGGER.info("Iniciando Stage...")
        if "description" in config:
            self.description = config.get("description")
            LOGGER.info(self.description)
        import_map = InputEngineUtils.import_loader(config)
        self.config = import_map.get("config")
        self.template_vars = import_map.get("template_vars")
        self.config = InputEngineUtils.process_template_vars(self.config,self.template_vars)
        self.config = InputEngineUtils.process_template_vars(self.config,process.process_vars)
        self.process = process
        self.enable = False
        if "enable" in config:
            if config.get("enable") == "true":
                self.enable = True
        self.inputs = InputEngineUtils.get_inputs(self.config.get("inputs"))
        self.steps = self.config.get("steps")
        LOGGER.info("Stage initializada!")
    
    def execute(self):
        if self.enable:
            LOGGER.info("Ejecutando Steps...")
            for step_config in self.steps:
                step = Step(self,step_config)
                tdf = hive.tables().filter("isTemporary = True").collect()
                LOGGER.debug("Tablas en memoria para la ejecucion del step")
                for t in tdf:
                    count = str(hive.table(t["tableName"]).count())
                    LOGGER.debug("\tTabla: "+t["tableName"]+" Registros: "+count)
                step.execute()

class Process():
    def __init__(self, config):
        global LOGGER
        self.process_vars = {}
        if "description" in config:
            self.description = config.get("description")
            LOGGER.info(self.description)
        if "process_vars" in config:
            self.process_vars = config.get("process_vars")
            config = InputEngineUtils.process_template_vars(config,self.process_vars)
        self.name = config.get("process_name")
        LOGGER = Logger(self.name)
        self.stages = config.get("stages")
        self.hive_database = config.get("hive_database")
        DataFrameEngineUtils.execute_query("use "+self.hive_database)
        LOGGER.info("Iniciando proceso...")
    
    def execute(self):
        LOGGER.info("Ejecutando Stages...")
        for stage_config in self.stages:
            stage = Stage(self,stage_config)
            stage.execute()
        
if __name__ == "__main__":
    with open("conf.json") as f_in:
        process_config = json.load(f_in)
    process = Process(process_config)
    process.execute()