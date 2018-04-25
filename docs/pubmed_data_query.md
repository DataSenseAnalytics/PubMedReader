# PubMed Root

https://www.nlm.nih.gov/databases/download/pubmed_medline.html

## PubMed API

### Get a full xml records for an article by ID:

E.g.

https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id=24838656

Result has the same format as the data dump

### Search:
A good starting point:
http://www.fredtrotter.com/2014/11/14/hacking-on-the-pubmed-api/

Example querying for records with leucovorin in any field published between 2016 and 2018:
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmode=json&retmax=20&sort=relevance&term=leucovorin%20AND%202016:2018[pdat]

Can use history to fetch results from search:

E.g. search for term 'cancer'
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&sort=relevance&term=cancer[mesh]&usehistory=y&reldate=1095

Use webenv and query key to fetch from history:
https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&WebEnv=NCID_1_132111043_130.14.22.215_9001_1524165929_1105307050_0MetA0_S_MegaStore&query_key=1&retmode=xml

Other option for fetching results is to use idlist in search result, but this is limited by `retmax` (at most 10000)

### Biopython
Can use Entrez component of Biopython package instead of urls:
E.g. search
```
def search(query):
    Entrez.email = 'your.email@example.com'
    handle = Entrez.esearch(db='pubmed',
                            sort='relevance',
                            retmax='20',
                            retmode='xml',
                            reldate='1095',
                            term=query,
                            field='mesh')
    results = Entrez.read(handle)
    return results
```
`query` format is somewhat flexible here, can use spaces and for example either `AND` syntax or `&`

to fetch:
```
Entrez.efetch(db='pubmed',
              retmode='xml',
              id=ids)
```

### API Doc
[Entrez Programming Utilities Help](https://www.ncbi.nlm.nih.gov/books/NBK25501/)

### All tools
https://dataguide.nlm.nih.gov/
