from smv import *
from smv.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext
import re

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

def pubMedCitation(path):

    df = smv.SmvApp.getInstance().sqlContext.read.format('com.databricks.spark.xml'
        ).options(rowTag='MedlineCitation'
        ).load(path
        ).where(col('Article.AuthorList').isNotNull())

    def exists(df, spath):
        fields = getFields(df, '')
        if (len(spath) == 1 and spath[0] in fields):
            return True
        if (spath[0] not in fields):
            return False
        else:
            return exists(df.select(spath[0] + '.*'), spath[1:])

    def getCol(df, path):
        if (exists(df, path.split('.'))):
            return col(path)
        else:
            return lit(None)


    def getFields(df, path):
        if (path == ''):
            return [f.name for f in df.schema.fields]
        else:
            return [f.name for f in df.select(path + '.*').schema.fields]


    def toArr(col):
        return udf(lambda s: s.split('|') if s is not None else None)(col)


    def arrCat(col):
        def _cat(c):
            if c is None or reduce(lambda x, y: x and y, [s is None for s in c]):
                return None
            else:
                a = [s for s in c if s is not None]
                return reduce(lambda x, y: x + '|' + y, a)
        return udf(_cat)(col)


    def ListCol(df, colName, path, fieldOp1, fieldOp2):
        if (reduce(lambda x, y: x or y, [colName in str(a) for a in df.schema.fields])): ##
            if ('ArrayType' in str(df.select(path).schema)):
                return arrCat(col(path + fieldOp1))
            else:
                return concat(*[col(path + f) for f in fieldOp2])
        else:
            return lit(None).cast('string')


    def applyMap(hashmap, key, default=''):
        """
        returns the map's value for the given key if present, and the default otherwise
        """
        def inner(k):
            if (k in hashmap):
                return hashmap[k]
            else:
                return default

        _fudf = udf(inner)
        return _fudf(key)

    seasonMap = {
        'Spring':'03',
        'Summer' :'06',
        'Autumn':'09',
        'Fall':'09',
        'Winter':'12'
    }

    monthMap = {
        'Jan':'01',
        'Feb':'02',
        'Mar':'03',
        'Apr':'04',
        'May':'05',
        'Jun':'06',
        'Jul':'07',
        'Aug':'08',
        'Sep':'09',
        'Oct':'10',
        'Nov':'11',
        'Dec':'12'
    }

    def mapSeason(season):
        if(season is None):
            return None
        else:
            return seasonMap(season)

    def getDate(d, prefix):
        return concat(
            coalesce(getCol(d, prefix + '.Year'), lit('')),
            lit('-'),
            coalesce(
                lpad(applyMap(monthMap, getCol(d, prefix + '.Month'), None), 2, '0'),
                applyMap(seasonMap, getCol(d, prefix + '.Season'), None), lit('01')
            ),
            lit('-'),
            coalesce(lpad(getCol(d, prefix + '.Day'), 2, '0'), lit('01'))
        ).cast('string')

    def getArticleDate(d):
        if exists(d, 'Article.ArticleDate'.split('.')):
            return when((getCol(df, 'Article.ArticleDate.Year').isNotNull()) & (length(trim(getCol(df, 'Article.ArticleDate.Year'))) > 0), getDate(d, 'Article.ArticleDate')).otherwise(lit(None).cast('string'))
        else:
            return lit(None).cast('string')

    articleDate = getArticleDate(df)

    journalDate = getDate(df, 'Article.Journal.JournalIssue.PubDate')

    res = df.select(
        concat(col('PMID._VALUE'), lit('_'), col('PMID._VERSION')).alias('PMID'),
        concat(col('Article.Journal.ISSN._IssnType'), lit('_'), col('Article.Journal.ISSN._VALUE')).alias('Journal_ISSN'),
        col('Article.ArticleTitle').alias('Article_Title'),
        col('Article.Journal.Title').alias('Journal_Title'),
        coalesce(articleDate, journalDate).alias('Journal_Publish_Date'),
        col('MedlineJournalInfo.Country').alias('Journal_Country'),
        ListCol(df, 'Grant', 'Article.GrantList.Grant', '.GrantID', ['.GrantID']).alias('Grant_Ids'),
        ListCol(df, 'MeshHeading', 'MeshHeadingList.MeshHeading', '.DescriptorName._UI', ['.DescriptorName._VALUE']).alias('Mesh_Headings'),
        ListCol(df, 'Chemical', 'ChemicalList.Chemical', '.NameOfSubstance._UI', ['.RegistryNumber', '.NameOfSubstance._UI']).alias('Chemicals'),
        explode('Article.AuthorList.Author').alias('Authors')
    )

    authorInfo = [getCol(res, 'Authors.' + f) for f in list(set(getFields(res, 'Authors')) - set(['Identifier', 'AffiliationInfo', 'CollectiveName']))]

    return res.smvSelectPlus(
        ListCol(res, 'Affiliation', 'Authors.AffiliationInfo.Affiliation', '', ['']).alias('Affiliation'),
        concat(getCol(df, 'Authors.Identifier.attr_Source'), lit('_'), getCol(df, 'Authors.Identifier._VALUE')).cast('string').alias('Author_Identifier'),
        *authorInfo
    ).drop('Authors')
