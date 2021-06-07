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

[ $# -ne 1 ] && msg error "Please give a sql file." && exit 1

sql_file=$1
[ ! -r "$sql_file" ] && msg error "Could not read sql file $sql_file" && exit 1

white_list="INSERT UPDATE DELETE ALTER RENAME GRANT REVOKE ANALYZE"
#white_list="ALTER RENAME GRANT REVOKE ANALYZE"
#white_list="INSERT UPDATE DELETE"
all_type=(`awk '{print $1}' $sql_file | sort | uniq`)
all_type_new=('')
i=0
for t1 in ${all_type[*]}
do
    for t2 in $white_list
    do
        if [ "$t1" == "$t2" ]; then
            all_type_new[$i]=$t1
            let i++
        fi
    done
done
msg info "ALL TYPE: ${all_type_new[*]}"

for t in ${all_type_new[*]}
do
    echo
    msg info "$t SORT"
    awk '$1~/'$t'/{print $0}' $sql_file | awk -r -F '[( ]+' \
           '{if($1~/INSERT/){print $1,$3} 
        else if($1~/UPDATE/){print $1,$2} 
        else if($1~/DELETE/){print $1,$3}
        else if($1~/ALTER/){print $1,$3}
        else if($1~/RENAME/){print $0}
        else if($1~/GRANT/){print $0}
        else if($1~/REVOKE/){print $0}
        else if($1~/ANALYZE/){print $1,$3}
        }' | sort | uniq -c | sort -rh -k 1,2 | head
done
