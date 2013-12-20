
--DROP TABLE Advertisers;
CREATE TABLE Advertisers(advertiserId int NOT NULL,
budget float, ctc float,balanceG float, displaycountG int, balanceG2 float, displaycountG2 int,
balanceB float, displaycountB int, balanceB2 float, displaycountB2 int, balanceP float, displaycountP int,
balanceP2 float, displaycountP2 int,
PRIMARY KEY(advertiserId));


--DROP TABLE Keywords;
CREATE TABLE Keywords(advertiserId int,
keyword VARCHAR(100), bid float,
PRIMARY KEY(advertiserId, keyword),
FOREIGN KEY (advertiserId) references Advertisers(advertiserId));


--DROP TABLE Queries;
CREATE TABLE Queries(qid integer NOT NULL,
query VARCHAR(400),PRIMARY KEY(qid));


--The main table. All the data related to a query is stored in this table.
--DROP TABLE QueryData;
CREATE TABLE QueryData
(AdvertiserId int,
 qId int,
 HitsCount int,
 KeywordCount int,
 QueryCount int,
 Similarity float,
 CTC float,
 QualityScore float,
 SumBid float,
  budget float,
 balanceG float,
 balanceG2 float,
 balanceB float,
 balanceB2 float,
 balanceP float,
 balanceP2 float,
 displayedCountG int,
 displayedCountG2 int,
 displayedCountB int,
 displayedCountB2 int,
 displayedCountP int,
 displayedCountP2 int,
 RankG float,
 RankG2 float,
 RankB float,
 RankB2 float,
 RankP float,
 RankP2 float
 );






--DROP TABLE matchedAdvertisers;
CREATE TABLE matchedAdvertisers (AdvertiserId int , qId int , HitsCount int , SumBid float , CTC float , budget float);


--DROP TABLE QuerytokenCnt;
CREATE TABLE QuerytokenCnt (qId int , KeywordCount int);


--DROP TABLE greedyoutput;
create table greedyoutput(qid int, rank int, aid int, balance float, budget float);

--DROP TABLE greedyoutput2;
create table greedyoutput2(qid int, rank int, aid int, balance float, budget float);

--DROP TABLE balanceOutput;
create table balanceOutput(qid int, rank int, aid int, balance float, budget float);

--DROP TABLE balanceOutput2;
create table balanceOutput2(qid int, rank int, aid int, balance float, budget float);


--DROP TABLE psiOutput;
create table psiOutput(qid int, rank int, aid int, balance float, budget float);

--DROP TABLE psiOutput2;
create table psiOutput2(qid int, rank int, aid int, balance float, budget float);

create table querytokens(qid int, token varchar(100));




CREATE OR REPLACE PROCEDURE createtokens(i IN int)
AS
BEGIN
   
declare 
q_id integer;

begin

delete from querytokens;

insert into querytokens (qid, token)
WITH test AS
	(SELECT q.qid qid, q.query str FROM Queries q WHERE q.qid = i )
	SELECT qid,
  	regexp_substr(str, '[^ ]+', 1, level) TOKEN
  	from test
	CONNECT by level <= length(regexp_replace (str, '[^ ]+')) + 1;
  end;
commit;
end;

/



--create data for a query.
CREATE OR REPLACE PROCEDURE createData
AS

begin

delete from matchedAdvertisers;
INSERT INTO matchedAdvertisers
SELECT A.AdvertiserId, T.qId, count(*), null, A.CTC, A.budget
FROM Advertisers A, Keywords K, Querytokens T
WHERE A.AdvertiserId = K.AdvertiserId AND K.keyword = T.token
GROUP BY A.AdvertiserId, T.qId, A.CTC, A.budget;



UPDATE matchedAdvertisers M SET sumBid =  (select sum( k.bid) from Advertisers A,
keywords K,  (SELECT DISTINCT TOKEN FROM querytokens) T
WHERE A.AdvertiserId = K.AdvertiserId AND K.keyword = T.token AND  M.advertiserId = k.AdvertiserId
GROUP BY A.AdvertiserId)  ;

-- to do can change this
delete from QuerytokenCnt;
INSERT INTO QuerytokenCnt
SELECT P.qId, sum(P.Powers)
FROM (SELECT T.qId, (power(count(*),2)) AS Powers
 FROM Querytokens T
 GROUP BY qId, token) P
GROUP BY qId;



delete from QueryData;
INSERT INTO QueryData
SELECT M.AdvertiserId, M.qId, M.HitsCount, K.KeywordCount, Q.KeywordCount, null, M.CTC, null, M.SumBid, M.budget, A.balanceG, A.balanceG2, A.balanceB, A.balanceB2, A.balanceP, A.balanceP2, A.displaycountG,  A.displaycountG2,  A.displaycountB,  A.displaycountB2, A.displaycountP, A.displaycountP2, null,null,null,null,null,null 
FROM matchedAdvertisers M, AdvertisersKeywordCount K, QuerytokenCnt Q, Advertisers A
WHERE M.AdvertiserId = K.AdvertiserId AND M.AdvertiserId = A.AdvertiserId AND M.qId = Q.qid ;



UPDATE QueryData SET Similarity=HitsCount/(SQRT(QueryCount*KeywordCount));
UPDATE QueryData SET QualityScore = CTC*Similarity;
UPDATE QueryData SET RankG = QualityScore*SumBid;

UPDATE QueryData SET RankG2 = QualityScore*SumBid;

UPDATE QueryData SET RankB = QualityScore*BalanceB;

UPDATE QueryData SET RankB2 = QualityScore*BalanceB2;

UPDATE QueryData SET RankP = QualityScore*SumBid*(1-EXP(-BalanceP/Budget));

UPDATE QueryData SET RankP2 = QualityScore*SumBid*(1-EXP(-BalanceP2/Budget));

commit;
end;

/

CREATE OR REPLACE PROCEDURE processAds(k1 IN int, k2 in int, k3 in int, k4 in int, k5 in int, k6 in int)
IS
   
       Rank integer;
       Bid float;
       nextBid float;
       queryCount int;
       timestart float;


BEGIN
        select count(*) into querycount from queries;

        FOR i IN 77..100

        LOOP

	   -- dbms_output.enable; 
    	    timestart:=dbms_utility.get_time(); 
            createtokens(i);
	    dbms_output.put_line(dbms_utility.get_time()-timestart); 
	    timestart:=dbms_utility.get_time(); 
	    createData;
	    dbms_output.put_line(dbms_utility.get_time()-timestart); 
	    
	     timestart:=dbms_utility.get_time(); 
	    Rank:= 1;
            FOR currentRecord IN(
            select * from 
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceG
            ORDER BY QueryData.RankG DESC,  advertiserId )
            where rownum <=k1)

            LOOP

                IF (currentRecord.displayedCountG < 100) THEN
                    UPDATE Advertisers
                    SET displayCountG = displayCountG +1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    currentRecord.displayedCountG := currentRecord.displayedCountG + 1;
                ELSE            
                    UPDATE Advertisers
                    SET displayCountG = 1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    currentRecord.displayedCountG := 1; 
                END IF;


                IF (currentRecord.CTC*100 >= currentRecord.displayedCountG) THEN
                    UPDATE Advertisers
                    SET BalanceG = BalanceG - currentRecord.Sumbid
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    Bid := currentRecord.Sumbid;
                ELSE
                    Bid := 0;
                END IF;

                      

                        
                insert into greedyOutput values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceG - Bid, currentRecord.budget);
               -- dbms_output.put_line(currentRecord.Advertiserid);
                Rank := Rank + 1;
            
                

            END LOOP;
        

            --- Start of insertion of second price greedy alogrithm top 5 ad's insertion into output table
            Rank:=1;
            nextBid:= 0;

            FOR currentRecord IN(  select * from 
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceG2 
            ORDER BY QueryData.RankG2 DESC, advertiserId )
            where rownum <=k2)
            

            LOOP

                IF (currentRecord.displayedCountG2 < 100) THEN
                    UPDATE Advertisers
                    SET displayCountG2 = displayCountG2 +1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    currentRecord.displayedCountG2 := currentRecord.displayedCountG2 + 1;
                ELSE
                    UPDATE Advertisers
                    SET displayCountG2 = 1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                      currentRecord.displayedCountG2:=1;
                END IF;

                IF (currentRecord.CTC*100 >= currentRecord.displayedCountG2) THEN
                    begin   
                        SELECT MAX(sumbid) into nextBid FROM QueryData WHERE sumbid IN(
                        SELECT sumbid FROM QueryData WHERE sumbid NOT IN
                        (SELECT sumbid FROM QueryData WHERE sumbid>= currentRecord.sumbid ))
                        AND balanceG2 >= sumbid;
                        --dbms_output.put_line(nextBid);
                        IF nextBid IS NULL  then
                          nextBid:= currentRecord.sumbid;
                        --dbms_output.put_line(nextBid);
                        end if;
                    end;
                    UPDATE Advertisers
                    SET balanceG2 = balanceG2 - nextBid
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;        
                ELSE
                    nextBid:= 0;
                END IF;

                
                insert into greedyOutput2 values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceG2 - nextBid, currentRecord.budget);
             --   dbms_output.put_line(currentRecord.Advertiserid);
                Rank := Rank + 1;
                
            END LOOP;
        

            --- Start of insertion of first price balance alogrithm top 5 ad's insertion into output table
            Rank:=1;
            
            FOR currentRecord IN(
             select * from 
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceB
            ORDER BY QueryData.RankB DESC, advertiserId )
            where rownum <=k3)

            LOOP

                IF (currentRecord.displayedCountB < 100) THEN
                    UPDATE Advertisers
                    SET displayCountB = displayCountB +1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    currentRecord.displayedCountB := currentRecord.displayedCountB + 1;
                ELSE
                    UPDATE Advertisers
                    SET displayCountB = 1
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    currentRecord.displayedCountB:=1;
                END IF;

                IF (currentRecord.CTC*100 >= currentRecord.displayedCountB) THEN
                    UPDATE Advertisers
                    SET balanceB = balanceB - currentRecord.Sumbid
                    WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                    Bid := currentRecord.Sumbid;
                ELSE
                    Bid := 0;
                END IF;
                  
                insert into balanceOutput values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceB - Bid, currentRecord.budget);
               -- dbms_output.put_line(currentRecord.Advertiserid);
                Rank := Rank + 1;

            END LOOP;
            

            --- Start of insertion of second price balance alogrithm top 5 ad's insertion into output table
            Rank:=1;
            nextBid:= 0;

            FOR currentRecord IN( select * from
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceB2
            ORDER BY QueryData.RankB2 DESC, advertiserId )
            where rownum <=k4)
           

            LOOP
        
            IF (currentRecord.displayedCountB2 < 100) THEN
                UPDATE Advertisers
                SET displayCountB2 = displayCountB2 +1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountB2 := currentRecord.displayedCountB2 + 1;
            ELSE
                UPDATE Advertisers
                SET displayCountB2 = 1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountB2:=1;
            END IF;

            IF (currentRecord.CTC*100 >= currentRecord.displayedCountB2) THEN
                begin   
                    select max(sumbid) into nextBid from QueryData where sumbid in(
                    select sumbid from QueryData where sumbid NOT IN
                    (select sumbid from QueryData where sumbid>= currentRecord.sumbid )) and balanceB2 >= sumbid;
                    dbms_output.put_line(nextBid);
                    IF nextBid IS NULL  then
                      nextBid:= currentRecord.sumbid;
                      dbms_output.put_line(nextBid);
                    end if;
                end;
                UPDATE Advertisers
                SET balanceB2 = balanceB2 - nextBid
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;    
            ELSE
                nextBid := 0;
            END IF;
            
            insert into balanceOutput2 values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceB2- nextBid, currentRecord.budget); 
            --dbms_output.put_line(currentRecord.Advertiserid);
            Rank := Rank + 1;

            END LOOP;
                


            --- Start of insertion of first price generalizedbalance alogrithm top 5 ad's insertion into output table
	   
	    Rank := 1;
	    
            FOR currentRecord IN( select * from
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceP
            ORDER BY QueryData.RankP DESC, advertiserId )
            where rownum <=k5)

            LOOP

            IF (currentRecord.displayedCountP < 100) THEN
                UPDATE Advertisers
                SET displayCountP = displayCountP +1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountP := currentRecord.displayedCountP + 1;
            ELSE
                UPDATE Advertisers
                SET displayCountP = 1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountP:=1;
            END IF;

            IF (currentRecord.CTC*100 >= currentRecord.displayedCountP) THEN
                UPDATE Advertisers
                SET BalanceP = BalanceP - currentRecord.Sumbid
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                Bid := currentRecord.Sumbid;
            ELSE
                Bid := 0;

            END IF;

        
            insert into psiOutput values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceP - Bid, currentRecord.budget);
           -- dbms_output.put_line(currentRecord.Advertiserid);
            Rank := Rank + 1;
        
            END LOOP;
        


            Rank:=1;
            nextBid:= 0;
            FOR currentRecord IN( select * from
            (select * FROM QueryData
            WHERE QueryData.SumBid <= QueryData.balanceP2
            ORDER BY QueryData.RankP2 DESC, advertiserId )
            where rownum <=k6)

            LOOP
            
            IF (currentRecord.displayedCountP2 < 100) THEN
                UPDATE Advertisers
                SET displayCountP2 = displayCountP2 +1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountP2 := currentRecord.displayedCountP2 + 1;
            ELSE
                UPDATE Advertisers
                SET displayCountP2 = 1
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
                currentRecord.displayedCountP2:=1;
            END IF;

            IF (currentRecord.CTC*100 >= currentRecord.displayedCountP2) THEN
                begin   
                    select max(sumbid) into nextBid from QueryData where sumbid in(
                    select sumbid from QueryData where sumbid NOT IN
                    (select sumbid from QueryData where sumbid>= currentRecord.sumbid )) and balanceP2 >= sumbid;
                    dbms_output.put_line(nextBid);
                    IF nextBid IS NULL  then
                          nextBid:= currentRecord.sumbid;
                          dbms_output.put_line(nextBid);
                    end if;
                end;
                UPDATE Advertisers
                SET balanceP2 = balanceP2 - nextBid
                WHERE Advertisers.AdvertiserId = currentRecord.AdvertiserId;
            ELSE
                nextBid := 0;
                END IF;

                insert into psiOutput2 values(currentRecord.qid, Rank , currentRecord.AdvertiserId, currentRecord.balanceP2 - nextBid, currentRecord.budget);
             --   dbms_output.put_line(currentRecord.Advertiserid);
                Rank := Rank + 1;
                

            END LOOP;
        END LOOP;
	 dbms_output.put_line(dbms_utility.get_time()-timestart); 
commit;
    
END;
--end displaygreedyad;
/







exit;


 
			



