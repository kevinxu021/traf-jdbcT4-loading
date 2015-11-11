#/bin/sh
echo $0
bin=`dirname "$0"`
base_dir=`cd "${bin}/../">/dev/null; pwd`
lib_dir="${base_dir}/lib"
lib_cp="${CLASSPATH}"
for jar in `ls ${lib_dir}/*.jar`
do
  lib_cp=$lib_cp:$jar
done

java -cp "${lib_cp}" -DconfigFile=${base_dir}/config.properties com.esgyn.jdb.Loading
