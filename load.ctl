LOAD DATA INFILE 
'Advertisers.dat'
 into TABLE advertisers 
fields terminated by '\t'
TRAILING NULLCOLS
(advertiserId  , budget , ctc , balanceG ":budget", displaycountG constant 0, balanceG2 ":budget", displaycountG2 constant 0 
, balanceB ":budget", displaycountB constant 0,  balanceB2 ":budget", displaycountB2 constant 0,  balanceP ":budget", displaycountP constant 0,  balanceP2 ":budget", displaycountP2 constant 0 )

