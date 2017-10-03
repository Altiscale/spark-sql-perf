#!/bin/bash

curr_dir=$(dirname $0)
curr_dir=$(cd $curr_dir; pwd)

tpcds_resource_dir=$curr_dir/src/main/resources
tpcds_all_queries=$tpcds_resource_dir/tpcds_2_4
tpcds_query_dst=$curr_dir/src/main/scala/com/databricks/spark/sql/perf/tpcds
tpcds_query_template=$tpcds_resource_dir/TPCDS_2_4_template_Queries.scala
tpcds_scala_template=$tpcds_resource_dir/tpcds_repl_2_4_template.scala
<<<<<<< HEAD
scala_output_tar=$curr_dir/tpcds_2_4_pXX.scala.tar.gz
=======
>>>>>>> 8967e29d619a756e1b1dc675e6e9e0b9431c23a6

function usage {
cat << EOF
  This script splits the query from the $tpcds_all_queries directory into
<<<<<<< HEAD
  sub-directories, and generate the code as Scala objects. It also produce the
  scala file with prefix tpcds_2_4_pXX.scala for Spark REPL shell to execute in the
  current directory. A tar.gz $scala_output_tar is created.
  The results are stored in each sub-directory on HDFS based on the -o option.
  You can specify the target database to query via the -d option.
=======
    sub-directories, and generate the code as Scala objects. It also produce the
    scala file with prefix tpcds_2_4_pXX.scala for Spark REPL shell to execute in the
    current directory. The results are stored in each sub-directory on HDFS based on the -o option.
    You can specify the target database to query via the -d option.
>>>>>>> 8967e29d619a756e1b1dc675e6e9e0b9431c23a6

  Usage: 
	  $0 -o hdfs_output_dir -d tpcds_database_name

  Example:
	  $0 -o /tmp/tpcds_partial_results -d tpcds_bin_partitioned_parquet_10000
EOF
  exit 1
}

while getopts hd:o: opt
do
  case "$opt" in
    d) database_name="$OPTARG";;
    o) output_hdfs_dir="$OPTARG";;
    h) usage; exit 1;;
    ?) usage; exit 1;;
  esac
done

use_database=$database_name
store_qry_result=$output_hdfs_dir

for f in `seq -w 1 20`
do
  mkdir -p $tpcds_resource_dir/tpcds_2_4_p${f}
done

find $tpcds_all_queries -type f | sort | split -l 6 - xxxtpcds_

i=1
for f in `find . -type f -name "xxxtpcds_*" | sort`
do
  idx_code=
  qlist_tmp=""
  for ff in `cat $f`
  do
    bname=`echo $(basename $ff) | cut -d. -f1`
    qlist_tmp="$qlist_tmp,\"$bname\""
    if [ $i -le 9 ] ; then
      cp $ff $tpcds_resource_dir/tpcds_2_4_p0${i}/
      idx_code="p0${i}"
    else
      cp $ff $tpcds_resource_dir/tpcds_2_4_p${i}/
      idx_code="p${i}"
    fi
  done
  # Generate tpcds code
  qlist=`echo $qlist_tmp | sed "s/^,//"`
  cat $tpcds_query_template | \
    sed "s/REPLACEME/$idx_code/g" | \
    sed "s/LIST_TO_UPDATE/$qlist/g" > \
    $tpcds_query_dst/TPCDS_2_4_${idx_code}_Queries.scala.tmp
  cat $tpcds_query_dst/TPCDS_2_4_${idx_code}_Queries.scala.tmp | \
    sed "s/LIST_TO_UPDATE/$qlist/g" > \
    $tpcds_query_dst/TPCDS_2_4_${idx_code}_Queries.scala
  rm -f $tpcds_query_dst/TPCDS_2_4_${idx_code}_Queries.scala.tmp
  echo "ok - generated query object tpcds2_4_${idx_code}Queries in code, use \"val queries = tpcds.tpcds2_4_${idx_code}Queries\" in your Scala shell"
  # Generate Scala REPL code for execution and automation
  cat $tpcds_scala_template | \
    sed "s/QUERYIDX/$idx_code/g" | \
    sed "s:HDFSDEST:$store_qry_result:g" | \
    sed "s/TARGETDATABASE/$use_database/g" > \
		$curr_dir/tpcds_2_4_${idx_code}.scala
  let "i++"
done

rm -f xxxtpcds_*
pushd $curr_dir
tar -cvzf $scala_output_tar tpcds_2_4_p*.scala
popd
rm -f $curr_dir/tpcds_2_4_p*.scala
exit 0
