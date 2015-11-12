# traf-jdbcT4-loading
A common program for data loading from one database to another
######Build steps. if you already have a JDB Jar, please ignore this step  
mvn clean antrun:run package  
\#The output Jar will locat in folder target/

######Steps to run
\# database -> json file  
modify src/main/resources/config.properties as your own  
sh bin/startup.sh  
\#the number of output json files equals to tgz\_threads

\#json file -> Hive external table. Refs: [json-serde-1.3.6-jar-with-dependencies.jar](https://github.com/rcongiu/Hive-JSON-Serde)    
```
add jar /home/trafodion/kevin/json-serde-1.3.6-jar-with-dependencies.jar;  
```
```
CREATE EXTERNAL TABLE json_test(id int, weight string, name string, bday string)
COMMENT 'This is the staging page view table'
row format serde 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/traf_test/kevin';
```   

\#hive external table -> hive internal table  
```
add jar /home/trafodion/kevin/json-serde-1.3.6-jar-with-dependencies.jar;   
create table json_test2 as select * from json_test;
```   
\#check on Trafodion  
```
select * from hive.hive.json_test2;
```



-----
####Properties
######Source Side:
src\_driver=org.trafodion.jdbc.t4.T4Driver  
src\_url=jdbc\:t4jdbc\://10.10.10.36\:23400/:
src\_user=traf123
src\_pwd=zzz  

\#it allowed to specity a list of table name. Also it supports column mappings.  
\#E.g.: src\_table=aaa(timestamp),bbb(a,b,c),ccc => tgz\_tables=acc(tsp),bcc(e,f,g),ddd.   
\#aaa(timestamp) => acc(tsp) means the column names are the same in both tables exception timestamp from src.
src\_tables=aaa,bbb,ccc

\#the number of connections it is using for SELECT  
\#src\_threads=10  
######Shuffle  
cache\_rows=10000  
cache\_exceed\_sleep\_ms=10000

######Target Side:  
tgz\_file\_name=temp    

\#the number of connections it is using for insertion  
tgz\_threads=3  

\#the number of rows for a insertion batch  
tgz\_batch_size=1000  
