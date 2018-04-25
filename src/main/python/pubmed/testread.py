from smv import SmvRunConfig, SmvXmlFile
import pyspark.sql.functions as F
from lib.core import normalizeDf
from lib.xmlinput import *
import urllib
import urllib2
from lib.core import *
from Bio import Entrez
import tempfile

class PubMed2018Base(SmvXmlFile, SmvRunConfig):
    def fullPath(self):
        return self.smvGetRunConfig("xml_path")

    def fullSchemaPath(self):
        return 'lib/pubmed_mini_schema.json'

    def rowTag(self):
        return 'MedlineCitation'

    def isEphemeral(self):
        return False

    def run(self, df):
        start_year = self.smvGetRunConfig("start_year")
        return normalizeDf(df).where(F.col('Year') >= start_year)


class TestPubMedQuery(PubMedQuery):
    def queryTerms(self):
        return ['cancer']

    def relDays(self):
        return '50'
