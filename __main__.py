#!/usr/bin/env python
#

from __future__ import print_function
from os.path import expanduser, join, abspath

#import time
import json
import logging

from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark import SparkContext


"""

"""


if __name__ == "__main__":
    logging.getLogger("py4j").setLevel(logging.ERROR)

    with open("conf.json") as f_in:
        data = json.load(f_in)

    process_name = data.get("process_name")
    for stage in data.get("stages"):
        print(stage["stage_name"])
    
    sc =SparkContext()
    sqlContext = HiveContext(sc)
    sqlContext.sql("use pruebas_cuadre")
    sqlContext.sql("FROM p17 SELECT count(*)").show()