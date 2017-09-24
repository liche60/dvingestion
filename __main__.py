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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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
        self.table_state = False
        self.debug_state = False
        self.table_state_step = 0
        self.table_state_step_m = 100

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
        if self.debug_state:
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

    def count_dataframe_for_logging(self,dataframe):
        count = 0
        if self.debug_state:
            count = dataframe.count()
        return count

    def log_mem_table_state(self,step):
        if self.table_state:
            if step >= self.table_state_step and step <= self.table_state_step_m:
                tdf = hive.tables().filter("isTemporary = True").collect()
                self.debug("persist replace "+str(step)+", tablas en memoria")
                for t in tdf:
                    tmp = hive.table(t["tableName"])
                    self.debug("\tTabla: "+t["tableName"]+" Registros: ")
                    count = str(tmp.count())
                    self.debug("\tTabla: "+t["tableName"]+" Registros: "+count)

    def show_dataframe_console(self,dataframe):
        if self.debug_state:
            dataframe.show()

class DataFrameEngineUtils():

    @staticmethod
    def get_filtered_dataframe(dataframe,filters):
        LOGGER.debug(" *** Filtrando dataframe...")
        for filter_item in filters:
            count = LOGGER.count_dataframe_for_logging(dataframe)
            LOGGER.debug(" *** Registros antes del filtro: "+str(count))
            exp = filter_item.get("expression")
            LOGGER.debug("\t\t *** Filtro: "+exp)
            dataframe = dataframe.filter(exp)
            count = LOGGER.count_dataframe_for_logging(dataframe)
            LOGGER.debug(" *** Registros despues del filtro: "+str(count))
        return dataframe

    @staticmethod
    def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def persist_memory_dataframe(name,dataframe):
        tdf = hive.tables().filter("isTemporary = False").collect()
        fisica = False
        for t in tdf: 
            if name.lower() == t["tableName"].lower():
                fisica = True
                continue
        if not fisica:
            LOGGER.debug("Registrando en memoria la tabla: "+name+"!")
            dataframe.cache()            

            LOGGER.show_dataframe_console(dataframe)
            dataframe.registerTempTable(name)
        else:
            LOGGER.error("La tabla ya se encuentra creada en Hive, saliendo!")
            exit()

    @staticmethod
    def replace_table_hive_old(name,dataframe):
        id = DataFrameEngineUtils.id_generator()
        tmpTableName = name+"_tmp_"+id
        tmpMemTableName = name+"_"+id
        DataFrameEngineUtils.persist_memory_dataframe(tmpMemTableName,dataframe.filter("0 = 1"))
        tdf = hive.tables().filter("isTemporary = False")
        tableExist = tdf.filter(tdf["tableName"].rlike(("(?i)^"+name+"$"))).count()
        if tableExist == 0:
            LOGGER.info("La tabla "+name+" no existe, se creara en HIVE")
            DataFrameEngineUtils.execute_query("CREATE TABLE "+name+" as select * from "+tmpMemTableName)
            LOGGER.info("La tabla "+name+" se ha creado en Hive")
            dataframe.write.mode("append").format("json").saveAsTable(name)
        else:
            LOGGER.info("Creando tabla temporal "+tmpTableName)
            DataFrameEngineUtils.execute_query("CREATE TABLE "+tmpTableName+" as select * from "+tmpMemTableName)
            LOGGER.info("Insertando datos a "+tmpTableName)
            dataframe.write.mode("append").format("json").saveAsTable(tmpTableName)
            LOGGER.info("Eliminando tabla "+name)
            DataFrameEngineUtils.execute_query("DROP TABLE "+name)
            LOGGER.info("Renombrando table "+tmpTableName+" por "+name)
            DataFrameEngineUtils.execute_query("ALTER TABLE "+tmpTableName+" RENAME TO "+name)
            LOGGER.info("Consultando datos desde "+name)
            dataframe = DataFrameEngineUtils.execute_query("SELECT * FROM "+name)
            LOGGER.info("Cachando...")
            dataframe.cache()
            #LOGGER.info("Mostrando...")
            #dataframe.show()
            #LOGGER.info("Contando...")
            #LOGGER.info("datos "+str(dataframe.count()))
        hive.dropTempTable(tmpMemTableName)


    @staticmethod
    def replace_table_hive(name,dataframe):
        cantidad = dataframe.count()
        if cantidad > 0:
            id = DataFrameEngineUtils.id_generator()
            tmpTableName = name+"_tmp_"+id
            tmpMemTableName = name+"_"+id
            tdf = hive.tables().filter("isTemporary = False")
            tableExist = tdf.filter(tdf["tableName"].rlike(("(?i)^"+name+"$"))).count()
            if tableExist == 0:
                DataFrameEngineUtils.persist_memory_dataframe(tmpMemTableName,dataframe.filter("0 = 1"))
                LOGGER.info("La tabla "+name+" no existe, se creara en HIVE")
                DataFrameEngineUtils.execute_query("CREATE TABLE "+name+" as select * from "+tmpMemTableName)
                LOGGER.info("La tabla "+name+" se ha creado en Hive")
                dataframe.write.mode("append").format("json").saveAsTable(name)
                hive.dropTempTable(tmpMemTableName)
            else:
                LOGGER.info("La tabla "+name+" se truncara")
                DataFrameEngineUtils.execute_query("TRUNCATE TABLE "+name)
                LOGGER.info("Almacenando nuevos datos en "+name)
                dataframe.write.mode("append").format("json").saveAsTable(name)
        else:
            DataFrameEngineUtils.execute_query("TRUNCATE TABLE "+name)


    @staticmethod
    def append_table_hive(name,dataframe):
        cantidad = dataframe.count()
        if cantidad > 0:
            id = DataFrameEngineUtils.id_generator()
            tmpMemTableName = name+"_"+id
            tdf = hive.tables().filter("isTemporary = False")
            tableExist = tdf.filter(tdf["tableName"].rlike(("(?i)^"+name+"$"))).count()
            stage_udf = udf(lambda: STAGE_NAME, StringType())
            if tableExist == 0:
                DataFrameEngineUtils.persist_memory_dataframe(tmpMemTableName,dataframe.filter("0 = 1"))
                LOGGER.info("La tabla "+name+" no existe, se creara en HIVE")
                DataFrameEngineUtils.execute_query("CREATE TABLE "+name+" as select * from "+tmpMemTableName)
                DataFrameEngineUtils.execute_query("CREATE TABLE "+name+"_trace as select *,'' stage from "+tmpMemTableName)
                LOGGER.info("La tabla "+name+" se ha creado en Hive")
                dataframe.write.mode("append").format("json").saveAsTable(name)
                dataframetrace = dataframe.withColumn("stage", stage_udf())
                dataframetrace.show()
                dataframetrace.write.mode("append").format("json").saveAsTable(name+"_trace")
                hive.dropTempTable(tmpMemTableName)
            else:
                dataframe.write.mode("append").format("json").saveAsTable(name)
                dataframetrace = dataframe.withColumn("stage", stage_udf())
                dataframetrace.show()
                dataframetrace.write.mode("append").format("json").saveAsTable(name+"_trace")
        else:
            LOGGER.info("El data frame "+name+" no tiene datos para insertar, continuando")

    @staticmethod
    def persist_dataframe(name,method,dataframe):
        LOGGER.info("La tabla "+name+" se guardara permanentemente en HIVE")
        if method == "REPLACE":
            LOGGER.info("La tabla "+name+" sera sobreescrita en Hive")
            DataFrameEngineUtils.replace_table_hive(name,dataframe)
        elif method == "APPEND":
            LOGGER.info("La tabla "+name+" existe en Hive, se insertaran los con nuevos datos")
            DataFrameEngineUtils.append_table_hive(name,dataframe)
        LOGGER.info("La tabla "+name+" se persiste en Hive")
        result = DataFrameEngineUtils.execute_query("SELECT * FROM "+name)
        #result.show()

        


    @staticmethod
    def execute_query(query):
        LOGGER.debug(" *** Ejecutando query: "+query)
        dataframe = hive.sql(query)
        count = LOGGER.count_dataframe_for_logging(dataframe)
        LOGGER.debug("\t\t *** El Query retorno "+str(count)+" registros!")
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
            
            LOGGER.log_mem_table_state(12)

            DataFrameEngineUtils.persist_memory_dataframe(destination,input_df)
            
            LOGGER.log_mem_table_state(13)

        return inputs_result
    
    @staticmethod
    def process_outputs(outputs,dataframe):
        for output_item in outputs:

            LOGGER.log_mem_table_state(0)

            table = output_item.get("table")
            filters = output_item.get("filters")

            LOGGER.info("procesando output table "+table)
            #dataframe.show()



            dataframe_tmp = DataFrameEngineUtils.get_filtered_dataframe(dataframe,filters)

            LOGGER.log_mem_table_state(0)

            persist = output_item.get("persist")
            if persist == "TRUE":
                persist_method = output_item.get("persist_method")
                DataFrameEngineUtils.persist_dataframe(table,persist_method,dataframe_tmp)
            else:
                DataFrameEngineUtils.persist_memory_dataframe(table,dataframe_tmp)

        LOGGER.log_mem_table_state(8)



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
                dataframe.cache()
                dataframe.count()
                first = False
            else:
                dftmp = DataFrameEngineUtils.execute_query(query)
                dftmp.cache()
                dataframe.count()
                dataframe = dataframe.unionAll(dftmp)
                dataframe.cache()
                dataframe.count()
            LOGGER.show_dataframe_console(dataframe)
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
        LOGGER.show_dataframe_console(dataframe)
        LOGGER.info("Join ejecutado!")        
        return dataframe

class Step():
    def __init__(self, stage, config):

        LOGGER.log_mem_table_state(10)

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

        LOGGER.log_mem_table_state(11)

        self.inputs = InputEngineUtils.get_inputs(self.config.get("inputs"))
        self.outputs = self.config.get("outputs")

        LOGGER.log_mem_table_state(14)

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
        
        LOGGER.log_mem_table_state(9)
        
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

                LOGGER.log_mem_table_state(0)

                step.execute()
                #tdf = hive.tables().filter("isTemporary = True").collect()

                LOGGER.log_mem_table_state(0)

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
            tdf = hive.tables().filter("isTemporary = True").collect()
            LOGGER.debug("Limpiando cahce de spark!")
            hive.clearCache()
            LOGGER.debug("Limpiando dataframes en memoria!")
            for t in tdf:
                name = t["tableName"]
                LOGGER.debug("\t ** Limpiando tabla: !"+name)
                hive.dropTempTable(name)
            stage = Stage(self,stage_config)
            stage.execute()

        
if __name__ == "__main__":
    with open("conf.json") as f_in:
        process_config = json.load(f_in)
    process = Process(process_config)
    process.execute()