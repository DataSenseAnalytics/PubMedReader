### Run with full schema
```
----------------------
pubmed.pubmedcitation.TestPubMedCitation2
----------------------
08:29:23 PERSISTING: /data/bzhang/output/pubmed.pubmedcitation.TestPubMedCitation2_204b9c2d.csv
08:36:46 RunTime: 7 minutes, 22 seconds and 618 milliseconds, N: 2143650
```

### Run with simplified schema
```
----------------------
pubmed.pubmedcitation.TestPubMedCitation2
----------------------
08:58:00 PERSISTING: /data/bzhang/output/pubmed.pubmedcitation.TestPubMedCitation2_204b9c2d.csv
09:04:06 RunTime: 6 minutes, 6 seconds and 937 milliseconds, N: 2143650
```

### Run with mini schema
```
----------------------
pubmed.pubmedcitation.TestPubMedCitation2
----------------------
09:40:50 PERSISTING: /data/bzhang/output/pubmed.pubmedcitation.TestPubMedCitation2_204b9c2d.csv
09:46:24 RunTime: 5 minutes, 33 seconds and 645 milliseconds, N: 2143650
```

Since `TestPubMedCitation2` has 50 partitions, and the entire pubmed has 812
partitions, the entire run time should be about 1.5 hours when using predefined
schema.
