MASTER FINAL SMART SYNC DIFFERENTIAL CHECK


SELECT 
cc.publisher_campaign_id as child_camp,  
cc.child_groups as child_group_count,  
pc.publisher_campaign_id as parent_camp,  
pc.parent_groups as parent_group_count, 
cc.child_groups - pc.parent_groups as Differential 
FROM  
	(SELECT parent_id,publisher_campaign_id, 
	count(*) as child_groups  
	FROM publisher_groups pg  
	JOIN smart_sync_pairs ssp  ON pg.publisher_campaign_id = ssp.child_id 
	WHERE publisher_campaign_id IN  
		(SELECT child_id  
		FROM smart_sync_pairs  
		WHERE vo_type = 1  
		AND client_id = 51568) 
	AND publisher_group_status != 'DELETED'  
	GROUP BY 1) cc   
JOIN  
	(SELECT publisher_campaign_id, 
	count(*) as parent_groups  
	FROM publisher_groups pg  
	WHERE publisher_campaign_id IN  
		(SELECT parent_id  
		FROM smart_sync_pairs  
		WHERE vo_type = 1  
		AND client_id = 51568) 
	AND publisher_group_status != 'DELETED'  
	GROUP BY 1) pc   
ON cc.parent_id=pc.publisher_campaign_id  

WHERE pc.publisher_campaign_id in  
	(SELECT parent_id 
	FROM smart_sync_pairs 
	WHERE vo_type = 1  
	AND client_id = 51568) 
GROUP BY 1,3 
HAVING cc.child_groups != pc.parent_groups  
order by Differential LIMIT 100;

LEFT JOIN FIND THE NULL VALUES:

SELECT a.publisher_campaign_id,a.publisher_group_name,b.publisher_group_id, b.publisher_group_name
FROM 
	(SELECT publisher_group_id,publisher_campaign_id, publisher_group_name 
	FROM publisher_groups pg 
	WHERE publisher_campaign_id IN  
		(14640288,13955034,14582080,13955038,14535698,14535700,13955039,13955037,14640295,13951640,14640289,14640292,13951641,14534749,13955036,14533425,13958866,14582081,13958867,13958868,13958869,14535697,14535699,14559429,13848359,13903191,13848353,13848347,13847934,14666377,13847014,13847008,13902033,13846506,14559428,13901491,14664050,14192164,13896330,13896324,13896318,13888752,13853146,13848365) 
	AND publisher_group_status != 'DELETED'
	) b 
LEFT JOIN 
	(SELECT publisher_group_id,publisher_campaign_id,publisher_group_name 
	FROM publisher_groups pg 
	JOIN smart_sync_pairs ssp  ON pg.publisher_campaign_id = ssp.child_id 
	WHERE publisher_campaign_id IN  
		(SELECT child_id  
		FROM smart_sync_pairs  
		WHERE vo_type = 1  
		AND client_id = 51568) 
	AND publisher_group_status != 'DELETED'
	) a 
ON a.publisher_group_name=b.publisher_group_name 
WHERE a.publisher_group_name IS NULL
GROUP BY 1,2
order by 2;


TEST:
SELECT a.publisher_campaign_id,a.publisher_group_name,b.publisher_group_id, b.publisher_group_name
FROM (
SELECT publisher_group_id,publisher_group_name, publisher_campaign_id
FROM publisher_groups pg
WHERE publisher_campaign_id IN (14640288,13955034,14582080,13955038,14535698,14535700,13955039,13955037,14640295,13951640,14640289,14640292,13951641,14534749,13955036,14533425,13958866,14582081,13958867,13958868,13958869,14535697,14535699,14559429,13848359,13903191,13848353,13848347,13847934,14666377,13847014,13847008,13902033,13846506,14559428,13901491,14664050,14192164,13896330,13896324,13896318,13888752,13853146,13848365)
) a
LEFT JOIN
(
SELECT publisher_group_id,publisher_group_name, ssp.parent_id
FROM publisher_groups pg
JOIN smart_sync_pairs ssp
ON pg.publisher_campaign_id=  ssp.child_id
WHERE parent_id in (14640288,13955034,14582080,13955038,14535698,14535700,13955039,13955037,14640295,13951640,14640289,14640292,13951641,14534749,13955036,14533425,13958866,14582081,13958867,13958868,13958869,14535697,14535699,14559429,13848359,13903191,13848353,13848347,13847934,14666377,13847014,13847008,13902033,13846506,14559428,13901491,14664050,14192164,13896330,13896324,13896318,13888752,13853146,13848365)
) b 
ON a.publisher_group_name=b.publisher_group_name
GROUP BY 4,3
HAVING 4 IS NULL
order by 2;

TEST 2 WORKING
ss.kwcheck (){
if (argcheck $# FUNCNAME $1); then
RunOnProd "select p.publisher_group_id as parent_group,
pgc.publisher_group_name as parent_group_name,
pg.publisher_group_name as child_group_name,
c.publisher_group_id as child_group,
p.id as parent_kw_id,c.id as child_kw_id,
p.creation_date as parent_created,
c.creation_date as child_created,
s.object_version as linked_date,
p.keyword,p.keyword_type 
from keyword_instances c join smart_sync_pairs s on s.child_id=c.publisher_group_id 
join publisher_groups pg on c.publisher_group_id=pg.publisher_group_id 
join keyword_instances p on p.publisher_group_id=s.parent_id and p.keyword=c.keyword and p.keyword_type=c.keyword_type and p.id=$1 and p.client_id=c.client_id 
join publisher_groups pgc on p.publisher_group_id = pgc.publisher_group_id\G"

fi
}