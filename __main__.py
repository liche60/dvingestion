#
#

from __future__ import print_function
from os.path import expanduser, join, abspath

from pyspark.context import SparkContext
from pyspark.sql import HiveContext


"""

"""


if __name__ == "__main__":
    sqlContext = HiveContext(sc)
    sqlContext.sql("use pruebas_cuadre")
    sqlContext.sql("FROM p17 SELECT count(*)").show()