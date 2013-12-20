LOAD DATA INFILE
'Queries.dat'
 into TABLE queries
fields terminated by '\t'
 (qid, query char(400))



