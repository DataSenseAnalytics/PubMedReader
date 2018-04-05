from smv import *
from smv.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import re
from pubmed.core import pubMedCitation

class TestPubMed(SmvModule):
    def path(self):
        return "data/input/pubmed_sample.xml"

    def requiresDS(self):
        return []

    def run(self, i):
        return pubMedCitation(self.path())
