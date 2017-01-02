# OWNER: shiggins
# CREATED: 2016-04-01
# TABLES USED: publisher_accounts/client_accounts/creatives/groups/campaigns, smart_sync_pairs
#############################################
        
source /usr/local/marin/ops/apptools/scripts/dbinfo.sh

usage() {

cat << EOF

USAGE: $(basename $0) OPTIONS

***************IMPORTANT PLEASE READ NOTE(S)***************

Will query the publisher creatives table and find a publisher ID or Creative ID as well as its smart sync pairing child/parent
OPTIONS:

EOF
        
        }

pub.costcheck () {
if ( argcheck $# 5 $FUNCNAME ); then
    runonstagclient $2 "select keyword_instance_dim_id,time_id, the_date,
    sum(impressions),sum(publisher_clicks),sum(publisher_cost) 
    from keyword_instance_fact_$1_$2 ki join time_by_day_epoch time_by_day_epoch
    on ki.time_id = tbde.time_id
    where keyword_instance_dim_id =(select keyword_instance_dim_id 
        from keyword_instance_dim_$1_$2 where keyword_instance_id = $3) 
and time_id in (select time_id from time_by_day_epoch where the_date between '$4' and '$5') 
    group by 2 order by 2;"
else
    echo "Give a Customer ID, Client ID, Keyword ID, Start date and End date"
fi
	}
# parse opts
while getopts "c:d:t:h" Option
do
    case $Option in
        c ) cid=$OPTARG;;
        d ) num_days=$OPTARG;;
        t ) conv_type=$OPTARG;;
        h ) usage;
            exit;;
        * ) usage;
            exit;;
    esac
done
shift $(($OPTIND - 1))

# if no ID given, dump usage and quit
if [ -z "${1}" ]; then
usage
else

fi

main
fi
exit