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

### API Doc
[Entrez Programming Utilities Help](https://www.ncbi.nlm.nih.gov/books/NBK25501/)

### All tools
https://dataguide.nlm.nih.gov/
