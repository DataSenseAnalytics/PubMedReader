from test_support.smvbasetest import SmvBaseTest
import pyspark.sql.functions as F
import sys

SrcPath = "./src/main/python"
sys.path.append(SrcPath)

import lib.core as C

class CoreTest(SmvBaseTest):
    """Test functions in pubmed.core
    """
    def test_normalizeDf(self):
        input_xml = 'src/test/python/resources/data/pubmed1rec.xml'
        res = C.pubMedCitation(input_xml)

        exp = self.createDF("Year: String;PMID: String;Journal_ISSN: String;Article_Title: String;Journal_Title: String;Journal_Publish_Date: String;Journal_Country: String;Mesh_Headings: String;Keywords: String;Affiliation: String;Author_Identifier: String;LastName: String;ForeName: String;Suffix: String;Initials: String",
            """1997,9103774_1,Print_0882-5963,Parenting attitudes and behaviors of low-income single mothers with young children.,Journal of pediatric nursing,1997-04-01,UNITED STATES,D000328|D001290|D002648|D002675|D005260|D006801|D007223|D007407|D009034|D009035|D016487|D011203|D015406|D012959,,"College of Nursing, University of Kentucky, Lexington 40536-0232, USA.",,Sachs,B,,B;
                1997,9103774_1,Print_0882-5963,Parenting attitudes and behaviors of low-income single mothers with young children.,Journal of pediatric nursing,1997-04-01,UNITED STATES,D000328|D001290|D002648|D002675|D005260|D006801|D007223|D007407|D009034|D009035|D016487|D011203|D015406|D012959,,,,Pietrukowicz,M,,M;
                1997,9103774_1,Print_0882-5963,Parenting attitudes and behaviors of low-income single mothers with young children.,Journal of pediatric nursing,1997-04-01,UNITED STATES,D000328|D001290|D002648|D002675|D005260|D006801|D007223|D007407|D009034|D009035|D016487|D011203|D015406|D012959,,,,Hall,L A,,LA"""
            )

        #res.smvDumpDF()
        self.should_be_same(res, exp)

    def test_getMedlineDate(self):
        data =  self.createDF('MedlineDate:String',
            """1998 Dec-1999 Jan;
            2000 Spring-Summer;
            2000 Spring;
            2000 Dec 23- 30""")

        res = data.select(F.struct('MedlineDate').alias('a'))\
            .select(C.getDate('a', 'MedlineDate').alias('date'))

        exp = self.createDF("date: String",
            """1998-12-01;
            2000-03-01;
            2000-03-01;
            2000-12-23""")

        self.should_be_same(res, exp)
