# traf-jdbcT4-loading
A common program for data loading from one database to another
######Steps to run
mvn clean package  //package your project, the jar will be located in target  
modify src/main/resources/config.properties as your own  
java -DconfigFile=[absolute path]/config.properties jdb-xxx.jar com.esgyn.jdb.Loading  
 
