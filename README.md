# traf-jdbcT4-loading
A common program for data loading from one database to another
######Steps to run
mvn clean package  //package your project, the jar will be located in target  
modify src/main/resources/config.properties as your own  
java -DconfigFile=[absolute path]/config.properties jdb-xxx.jar com.esgyn.jdb.Loading  

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

######Target Side:  
tgz\_driver=org.trafodion.jdbc.t4.T4Driver  
tgz\_url=jdbc\:t4jdbc\://10.10.10.36\:23400/:    
tgz\_user=traf123  
tgz\_pwd=zzz  
\#if tgz_tables not exist, it'll use src_tables    
tgz\_tables=aaa,bbb,ccc  

\#the number of connections it is using for insertion
tgz\_threads=3  

\#the number of rows for a insertion batch
tgz\_batch_size=1000  
