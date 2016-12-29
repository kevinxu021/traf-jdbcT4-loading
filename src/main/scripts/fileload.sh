#!/bin/sh
currdir=`dirname "$0"`
currdir=`cd "$currdir">/dev/null; pwd`
date
java -jar ${currdir}/../lib/jdb.jar -f ${currdir}/../conf/config_file.properties