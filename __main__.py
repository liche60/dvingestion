#!/usr/bin/env python
#

from __future__ import print_function
from os.path import expanduser, join, abspath

#import time
import json
import logging

from pyspark.context import SparkContext
from pyspark.sql import HiveContext


"""

"""


if __name__ == "__main__":
    logging.getLogger("py4j").setLevel(logging.ERROR)

    with open("conf.json") as f_in:
        data = json.load(f_in)

    process_name = data.get("process_name")
    for stage in data.get("stages"):
        print(stage["stage_name"])

    sqlContext = HiveContext(sc)
    sqlContext.sql("use pruebas_cuadre")
    #sqlContext.sql("FROM "+table+" SELECT count(*)").show()