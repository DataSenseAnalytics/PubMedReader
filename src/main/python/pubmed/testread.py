from smv import SmvRunConfig, SmvXmlFile
import pyspark.sql.functions as F
from lib.core import normalizeDf


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
        return normalizeDf(df).where(F.col('Year') >= '2000')
