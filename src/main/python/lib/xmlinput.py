import abc
import sys
from smv import *
from smv.smvdataset import SmvDataSet
from pyspark.sql.types import StructType
from core import *
import urllib
import tempfile
import json

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

class SmvXmlInput(SmvDataSet, ABC):
    """SmvDataSet representing external XML input
    """

    def isEphemeral(self):
        return True

    def dsType(self):
        return "Input"

    def requiresDS(self):
        return []

    def run(self, df, i):
        """Post-processing for input data

            Args:
                df (DataFrame): input data

            Returns:
                (DataFrame): processed data
        """
        return df

    @abc.abstractproperty
    def path(self):
        """
        XML data file path (directory or file, could be gz files)
        """

    @abc.abstractproperty
    def schemaPath(self):
        """
        User defined schema file (JSON)
        """

    @abc.abstractproperty
    def rowTag(self):
        """
        XML tag to identify a row
        """

    def doRun(self, validator, known):
        # Read in schema
        import json
        with open (self.schemaPath(), "r") as sj:
            schema_st = sj.read()
        schema = StructType.fromJson(json.loads(schema_st))

        # Load XML
        df = self.smvApp.sqlContext\
            .read.format('com.databricks.spark.xml')\
            .options(rowTag=self.rowTag())\
            .load(self.path(), schema=schema)

        i = self._constructRunParams(known)
        result = self.run(df, i)
        self.assert_result_is_dataframe(result)
        return result._jdf

    def _constructRunParams(self, urn2df):
        """Given dict from urn to DataFrame, construct RunParams for module
            A given module's result may not actually be a DataFrame. For each
            dependency, apply its df2result method to its DataFrame to get its
            actual result. Construct RunParams from the resulting dict.
        """
        urn2res = {}
        for dep in self.dependencies():
            jdf = urn2df[dep.urn()]
            df = DataFrame(jdf, self.smvApp.sqlContext)
            urn2res[dep.urn()] = dep.df2result(df)
        i = self.RunParams(urn2res)
        return i


class PubMedQuery(SmvXmlInput, ABC):

    @abc.abstractproperty
    def queryTerms(self):
        """
        """

    @abc.abstractproperty
    def startDate(self):
        """
        """

    @abc.abstractproperty
    def endDate(self):
        """
        """

    def rowTag(self):
        return 'MedlineCitation'

    def search(self):
        searchResult = pubmedSearch(self.queryTerms(), self.startDate(), self.endDate())
        return searchResult

    def fetch(self):
        return fetchURL(self.search())

    def resultCnt(self):
        return pubmedCnt(self.queryTerms(), self.startDate(), self.endDate())

    def run(self, df, i):
        print self.resultCnt()
        print self.path()
        return normalizeDf(df)

    def schemaPath(self):
        return 'lib/pubmed_mini_schema.json'

    def path(self):
        qres = urllib.urlopen(self.fetch()).read()
        g = tempfile.NamedTemporaryFile(delete=False)
        g.write(qres)
        #print g.name
        g.close()
        return g.name


class ClinicalTrialFetch(SmvModule):

    def requiresDS(self):
        return []

    def run(self):
        trialdfs = []
        trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50'
        getTrial = urllib.urlopen(trial).read()
        f = 0
        while(getTrial != '{"Error":"Bad Request."}'):
            g = open('tempfile.txt','w+')
            g = tempfile.NamedTemporaryFile(delete=False)
            g.write(getTrial)
            g.flush()
            g.close()
            g = open('tempfile.txt','r+')

            trialdfs.append(sqlContext.read.json(g.name))
            f = f + 50
            trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50&from={0}'.format(f)
            g.close()
            getTrial = urllib.urlopen(trial).read()

            if os.path.exists(g.name):
                os.remove(g.name)

        #TODO include condition

        #return 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50'
        #convert json result to dict , dict to xml
        # qres = urllib.urlopen(self.fetch()).read()
        # resdict = json.loads(qres)
        # g = tempfile.NamedTemporaryFile(delete=False)
        # g.write(qres)
        # g.close()
        # return sqlContext.read.json(g.name)

        return reduce(lambda a, b: a.smvUnion(b), trialdfs)
