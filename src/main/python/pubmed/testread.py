from smv import *
from smv.functions import *
import pyspark.sql.functions as F
from lib.core import normalizeDf
from lib.xmlinput import SmvXmlInput

class PubMed2018Base(SmvXmlInput, SmvRunConfig):
    def path(self):
        return self.smvGetRunConfig("xml_path")

    def schemaPath(self):
        return 'lib/pubmed_mini_schema.json'

    def rowTag(self):
        return 'MedlineCitation'

    def isEphemeral(self):
        return False

    def run(self, df):
        return normalizeDf(df).where(F.col('Year') >= '2000')
