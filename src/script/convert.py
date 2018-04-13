from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import sys
from smv import SmvApp
SmvApp.createInstance(sys.argv[1:])

SrcPath = "./src/main/python"
sys.path.append(SrcPath)
import pubmed.core as C

def filename(id):
    return "/Users/bozhang/data/pubmed/pubmed18n{:04d}.xml.gz".format(id)

for i in xrange(1, 2):
    a = C.pubMedCitation(filename(i))
    a.smvExportCsv('pubmed18n{:04d}.csv'.format(i))
