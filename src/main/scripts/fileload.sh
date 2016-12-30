#!/bin/sh
currdir=`dirname "$0"`
currdir=`cd "$currdir">/dev/null; pwd`
jdb_name=`cd ${currdir}/../lib;ls trafjdb-*`
lib_dir="${currdir}/../lib"
lib_cp=
for jar in `cd ${lib_dir};ls *.jar`
do
  lib_cp=$lib_cp:${lib_dir}/$jar
done
date
java -cp ${lib_cp} org.trafodion.ci.loader.TrafLoader -f ${currdir}/../conf/config_file.properties
date
