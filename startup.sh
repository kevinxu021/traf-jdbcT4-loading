#/bin/sh
echo $0
bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
lib_dir="${bin}/../lib"
lib_cp="${CLASSPATH}"
for jar in `ls ${lib_dir}/*.jar`
do
  lib_cp=$lib_cp:$jar
done

java -cp "${lib_cp}" -DconfigFile=${bin}/../config.properties -jar ../jdb-0.9.1.jar
