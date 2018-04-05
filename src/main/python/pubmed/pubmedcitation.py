from smv import *
from smv.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import re
import abc
from pubmed.core import pubMedCitation

class BasePubMedCitation(SmvModule):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def range(self):
        """range"""

    def requiresDS(self):
        return []

    def run(self, i):
        nrange = self.range()
        paths = [("/data/pdda_raw/17_pubmed/2016baseline/medline16n{:04d}.xml.gz").format(n) for n in nrange]

        res = reduce(lambda a, b: a.smvUnion(b), map(pubMedCitation, paths))

        return res

class TestPubMedCitation(BasePubMedCitation, SmvOutput):

    def range(self):
        return range(10, 11)

class TestPubMedCitation2(BasePubMedCitation, SmvOutput):

    def range(self):
        return range(1, 50)

class PubMedCitation1(BasePubMedCitation):

    def range(self):
        return range(1, 100)

class PubMedCitation2(BasePubMedCitation):

    def range(self):
        return range(100, 200)

class PubMedCitation3(BasePubMedCitation):

    def range(self):
        return range(200, 300)

class PubMedCitation4(BasePubMedCitation):

    def range(self):
        return range(300, 400)

class PubMedCitation5(BasePubMedCitation):

    def range(self):
        return range(400, 500)

class PubMedCitation6(BasePubMedCitation):

    def range(self):
        return range(500, 600)

class PubMedCitation7(BasePubMedCitation):

    def range(self):
        return range(600, 700)

class PubMedCitation8(BasePubMedCitation):

    def range(self):
        return range(700, 812)

"""
Article_Title-same:Double        = 1.0
Journal_Publish_Date-same:Double = 0.7097306212204508
Suffix-same:Double               = 0.999953212543717
Journal_Title-same:Double        = 1.0
Journal_Country-same:Double      = 1.0
Journal_ISSN-same:Double         = 1.0
Mesh_Headings-same:Double        = 0.07131578023931784
Chemicals-same:Double            = 0.4140689881042892
Grant_Ids-same:Double            = 0.8630648123238159
Author_Identifier-same:Double    = 0.9996841846700899
Initials-same:Double             = 1.0

Article_Title-same:Double        = 1.0
Journal_Publish_Date-same:Double = 0.7395850469740478
Suffix-same:Double               = 0.9999269707166241
Journal_Title-same:Double        = 1.0
Journal_Country-same:Double      = 1.0
Journal_ISSN-same:Double         = 1.0
Mesh_Headings-same:Double        = 0.06804061567102727
Chemicals-same:Double            = 0.4147677420498195
Grant_Ids-same:Double            = 0.867387887407706
Author_Identifier-same:Double    = 0.9996677008153813
Initials-same:Double             = 0.9999999544421189
"""

class PubMedCitations(SmvModule, SmvOutput):
    def requiresDS(self):
        return [
            PubMedCitation1,
            PubMedCitation2,
            PubMedCitation3,
            PubMedCitation4,
            PubMedCitation5,
            PubMedCitation6,
            PubMedCitation7,
            PubMedCitation8
        ]

    def run(self, i):
        pub1 = i[PubMedCitation1]
        pub2 = i[PubMedCitation2]
        pub3 = i[PubMedCitation3]
        pub4 = i[PubMedCitation4]
        pub5 = i[PubMedCitation5]
        pub6 = i[PubMedCitation6]
        pub7 = i[PubMedCitation7]
        pub8 = i[PubMedCitation8]

        pubs = [pub1, pub2, pub3, pub4, pub5, pub6, pub7, pub8]

        return reduce(lambda a, b: a.smvUnion(b), pubs)
