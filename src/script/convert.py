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
    file_path = "/data/pdda_raw/17_pubmed/2018baseline"
    return "{}/pubmed18n{:04d}.xml.gz".format(file_path, id)

out_path = "/data/pdda_raw/17_pubmed/2018baseline_converted"
for i in xrange(200, 929):
    a = C.pubMedCitation(filename(i))
    a.smvExportCsv('{}/pubmed18n{:04d}.csv'.format(out_path, i))
