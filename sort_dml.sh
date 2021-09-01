#!/usr/bin/env bash


# define message color
msg() {
    case $1 in
        error)
            echo -e "\e[1;31m[`date +"%F %T"`] $2\e[0m";;
        info)
            echo -e "\e[1;32m[`date +"%F %T"`] $2\e[0m";;
    esac
}

[ $# -eq 0 ] && msg error "Please give a sql file." && exit 1

sql_file=$1
[ ! -r "$sql_file" ] && msg error "Could not read sql file $sql_file" && exit 1
head_lines=$2
tmp_file=/tmp/sort_dml.txt

grep -E "^$t" $sql_file | awk -r -F '[( ]+' \
       '{if($1~/INSERT/){array[$1"\t"$3]++} 
    else if($1~/UPDATE/){array[$1"\t"$2]++} 
    else if($1~/DELETE/){array[$1"\t"$3]++}
    else if($1~/ALTER/){array[$1"\t"$3]++}
    else if($1~/RENAME/){array[$0]++}
    else if($1~/GRANT/){array[$0]++}
    else if($1~/REVOKE/){array[$0]++}
    else if($1~/ANALYZE/){array[$1"\t"$3]++}
    }END{for(i in array) print array[i]"\t"i}' | sort -rh > $tmp_file

all_type=(`awk '{print $2}' $tmp_file | sort | uniq`)
for t in ${all_type[*]}
do
    echo
    msg info "$t SORT"
    awk '$2~/^'$t'/{print $0}' $tmp_file | head -n ${head_lines:=10}
done
