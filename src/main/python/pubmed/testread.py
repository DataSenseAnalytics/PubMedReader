from smv import SmvRunConfig, SmvXmlFile
import pyspark.sql.functions as F
from lib.core import normalizeDf
from lib.xmlinput import *
import urllib
import urllib2
from lib.core import *
from Bio import Entrez
import tempfile
import conf
import os
from smv import SmvApp
import json

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
        #q = conf.TestConf().TargetDrug() + conf.TestConf().TargetProcedure()
        q = conf.TestConf().TargetDrug()
        return q

    def startDate(self):
        #return '1575'
        return '2013'
        #return '2014/01/01'

    def endDate(self):
        return '2015'



class ClinicalTrialFetch(SmvModule):

    def requiresDS(self):
        return []

    def run(self, i):
        sql_ctx = SmvApp.getInstance().sqlContext #if (sqlContext is None) else sqlContext
        trialdfs = []
        trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50'
        #trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?exclude=eligibility&exclude=associated_studies&exclude=keywords&exclude=biomarkers&exclude=arm_description&size=50'

        getTrial = urllib.urlopen(trial).read()
        j = json.loads(getTrial)

        #f = 0
        f = 2000
        #while(len(j['trials']) > 0):
        while(f<3000):

            jtrials = reduce(lambda x, y: x + '\n' + y, map(lambda x: json.dumps(x), j['trials']))

            # def makeTrialDict(trialobj):
            #     trial = {}
            #     if f==2000:
            #         print trialobj.keys()
            #     if 'interventions' in trialobj.keys():
            #         interventionName = trialobj['interventions']['intervention_name']
            #         #interventionName = trialobj['interventions'][0]
            #         trial['intervention_name'] = interventionName
            #     trial['nct_id'] = trialobj['nct_id']
            #     trial['brief_summary'] = trialobj['brief_summary']
            #     trial['brief_title'] = trialobj['brief_title']
            #     trial['start_date'] = trialobj['start_date']
            #     trial['completion_date'] = trialobj['completion_date']
            #     return trial

            #jtrials = reduce(lambda x, y: x + '\n' + y, map(lambda x: json.dumps(makeTrialDict(x)), j['trials']))

            # if f==0:
            #     print jtrials
            #jtrials = json.dumps(t)
            g = open('tempfile.txt','w')
            #g = tempfile.NamedTemporaryFile(delete=False)
            # isolate trials content, write to file with newlines between each record. use spark.read.json(fname)
            g.write(jtrials)
            g.flush()
            g.close()

            trialdfs.append(self.smvApp.sqlContext.read.json(g.name))
            f = f + 50
            trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50&from={0}'.format(f)
            #trial = 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?exclude=eligibility&exclude=associated_studies&exclude=keywords&exclude=biomarkers&exclude=arm_description&size=50&from={0}'.format(f)

            #g.close()
            getTrial = urllib.urlopen(trial).read()
            #print getTrial
            j = json.loads(getTrial)
            # if os.path.exists(g.name):
            #     os.remove(g.name)

        #TODO include condition

        #return 'https://clinicaltrialsapi.cancer.gov/v1/clinical-trials?include=nct_id&include=brief_summary&include=brief_title&include=start_date&include=completion_date&include=overall_status&include=interventions.intervention_name&size=50'
        #convert json result to dict , dict to xml

        res = reduce(lambda a, b: a.smvUnion(b), trialdfs)

        return res
