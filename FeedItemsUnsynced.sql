Select pc.publisher_campaign_id, campaign_name, pf.ext_id, pf.status as Feed_status, feed_item_id as SLID, fi.ext_id as feed_ext, left(display_text,10) as text,uniq_id,fi.name, fi.operation_status  
from publisher_campaigns pc 

join publisher_feeds pf on pc.client_account_id = pf.client_account_id
join feed_items fi on pf.publisher_feed_id = fi.publisher_feed_id 
join sitelink_feed_items sfi on sfi.sitelink_feed_item_id = fi.feed_item_id 
where fi.feed_item_id IN (21518249,

;

URLs not present:
SELECT
fi.publisher_feed_id,
fi.object_version,
fi.creation_date,
pf.client_account_id,
sfi.destination_url as durl,
sfi.sitelink_feed_item_id as Sitelink_Id,
pa.client_id, pca.alias as Account, 
fi.operation_status as status,
sfi.display_text as Link_text,
sfi.description1 as Desc1,
sfi.description2 as Desc2
FROM marin.publisher_accounts pa 
JOIN marin.publisher_client_accounts pca on pca.publisher_account_id=pa.publisher_account_id 
JOIN marin.publisher_feeds pf on pca.client_account_id = pf.client_account_id
JOIN marin.feed_items fi on pf.publisher_feed_id = fi.publisher_feed_id
JOIN marin.sitelink_feed_items sfi ON fi.feed_item_id = sfi.sitelink_feed_item_id
WHERE 
pa.client_id = 44279 
and pca.alias like '%Google_EN|BP|BP|EAME|ME%';

AND sfi.sitelink_feed_item_id in (30420834,
30420837,
30420840,
30420842,
30420843,
30610352,
30610355,
31222060,
31223159,
31223160);



MariaDB [marin]> desc vo_container_feed_item_links;
+--------------------------------+-------------+------+-----+---------------------+-------+
| Field                          | Type        | Null | Key | Default             | Extra |
+--------------------------------+-------------+------+-----+---------------------+-------+
| vo_container_feed_item_link_id | bigint(20)  | NO   | PRI | NULL                |       |
| vo_feed_item_container_id      | bigint(20)  | YES  | MUL | NULL                |       |
| feed_item_id                   | bigint(20)  | YES  | MUL | NULL                |       |
| status                         | varchar(24) | YES  |     | NULL                |       |
| operation_status               | varchar(24) | YES  |     | NULL                |       |
| creation_date                  | timestamp   | NO   |     | CURRENT_TIMESTAMP   |       |
| object_version                 | timestamp   | NO   |     | 0000-00-00 00:00:00 |       |
+--------------------------------+-------------+------+-----+---------------------+-------+
7 rows in set (0.00 sec)

MariaDB [marin]> desc vo_feed_item_containers;
+---------------------------+-------------+------+-----+---------------------+-------+
| Field                     | Type        | Null | Key | Default             | Extra |
+---------------------------+-------------+------+-----+---------------------+-------+
| vo_feed_item_container_id | bigint(20)  | NO   | PRI | NULL                |       |
| linked_vo_id              | bigint(20)  | NO   | MUL | NULL                |       |
| vo_type                   | tinyint(3)  | NO   |     | NULL                |       |
| place_holder_type         | tinyint(3)  | YES  |     | NULL                |       |
| device_preference_type    | tinyint(3)  | YES  |     | NULL                |       |
| status                    | varchar(24) | YES  |     | NULL                |       |
| operation_status          | varchar(24) | YES  |     | NULL                |       |
| creation_date             | timestamp   | NO   |     | CURRENT_TIMESTAMP   |       |
| object_version            | timestamp   | NO   |     | 0000-00-00 00:00:00 |       |
+---------------------------+-------------+------+-----+---------------------+-------+

Select *
from publisher_campaigns pc 

join publisher_feeds pf on pc.client_account_id = pf.client_account_id
join feed_items fi on pf.publisher_feed_id = fi.publisher_feed_id 
join sitelink_feed_items sfi on sfi.sitelink_feed_item_id = fi.feed_item_id 
where fi.feed_item_id = 21518254; 

SELECT pca.alias Account, 
pc.campaign_name Campaign, 
pg.publisher_group_name 'Group', 
sfi.display_text 'Link Text', 
sfi.destination_url 'Destination URL', 
date_format(date(convert_tz(fi.start_time, 'UTC', mc.time_zone)), '%m/%d/%Y') 'Start Date', 
date_format(date(convert_tz(fi.end_time, 'UTC', mc.time_zone)), '%m/%d/%Y') 'End Date', 
CASE 
WHEN fi.device_preference_type = 0 THEN 'ALL' 
WHEN fi.device_preference_type = 1 THEN 'MOBILE' 
WHEN fi.device_preference_type = 2 THEN 'DESKTOP' 
WHEN fi.device_preference_type = 1 THEN 'TABLET' 
END AS 'Device Preference' 
FROM marin.publisher_accounts pa 
JOIN marin.clients mc USING (client_id) 
JOIN marin.publisher_client_accounts pca USING (publisher_account_id) 
JOIN marin.publisher_feeds pf USING (client_account_id) 
JOIN marin.feed_items fi USING (publisher_feed_id) 
JOIN marin.sitelink_feed_items sfi ON (fi.feed_item_id = sfi.sitelink_feed_item_id) 
LEFT JOIN marin.vo_container_feed_item_links link USING (feed_item_id) 
LEFT JOIN marin.vo_feed_item_containers container USING (vo_feed_item_container_id) 
LEFT JOIN marin.publisher_campaigns pc ON (container.linked_vo_id = pc.publisher_campaign_id) 
LEFT JOIN marin.publisher_groups pg ON (pg.publisher_campaign_id = pc.publisher_campaign_id 
OR container.linked_vo_id = pg.publisher_group_id) 
WHERE client_id = 44279
AND fi.feed_item_id = 1087517; 
AND AND (pc.publisher_campaign_status != 'DELETED') 
AND fi.operation_status <> 'synced';

SELECT count(*), fi.publisher_feed_id, 
pf.client_account_id, 
sitelink_feed_item_id as Sitelink_Id, 
pa.client_id, pca.alias as Account,  
fi.operation_status as status, 
sfi.display_text as Link_text, 
pc.campaign_name 
FROM marin.publisher_accounts pa  
JOIN marin.publisher_client_accounts pca on pca.publisher_account_id=pa.publisher_account_id  
JOIN marin.publisher_feeds pf on pca.client_account_id = pf.client_account_id 
JOIN marin.feed_items fi on pf.publisher_feed_id = fi.publisher_feed_id 
JOIN marin.sitelink_feed_items sfi ON fi.feed_item_id = sfi.sitelink_feed_item_id 
JOIN marin.vo_container_feed_item_links link USING (feed_item_id)  
JOIN marin.vo_feed_item_containers container USING (vo_feed_item_container_id)  
JOIN marin.publisher_campaigns pc ON (container.linked_vo_id = pc.publisher_campaign_id)  
WHERE   pa.client_id = 44279  
and fi.publisher_feed_id = 1087517 
and pc.campaign_name = 'US_EN_SI_3731_BP_EX_EAME_Sheraton Grand Dubai' 
Group by 4;
and fi.operation_status!='SYNCED' 
	;
OR sfi.display_text = 'Boots Star Gift Offer');


#GET DB Clicks using only PCLID

#Group level
SELECT count(*), 
if(action_id="1","Click","Conversion") as Actions, 
cast(receive_time as date) as date, 
pc.client_account_id Account, 
pc.campaign_name Campaign, 
pg.publisher_group_name Group_name 
FROM tracker_data_42420 td 
JOIN marin.publisher_groups pg on td.publisher_group_id=pg.publisher_group_id
JOIN marin.publisher_campaigns pc on pg.publisher_campaign_id=pc.publisher_campaign_id
JOIN marin.publisher_client_accounts pca on pc.client_account_id=pca.client_account_id
WHERE pc.client_account_id = 194139
AND receive_time BETWEEN '2015-07-29' AND '2015-07-30'
GROUP BY 2,3,4,5,6
LIMIT 50;

#Kw level
SELECT count(*), 
if(action_id="1","Click","Conversion") as Actions, 
cast(receive_time as date) as date, 
pc.client_account_id Account, 
pc.campaign_name Campaign, 
pg.publisher_group_name Group_name,
kw.keyword Keyword
FROM tracker_data_42420 td 
JOIN marin.keyword_instances kw on td.keyword_instance_id=kw.id
JOIN marin.publisher_groups pg on kw.publisher_group_id=pg.publisher_group_id
JOIN marin.publisher_campaigns pc on pg.publisher_campaign_id=pc.publisher_campaign_id
JOIN marin.publisher_client_accounts pca on pc.client_account_id=pca.client_account_id
WHERE pc.client_account_id = 194139
AND receive_time BETWEEN '2015-07-29' AND '2015-07-30'
GROUP BY 2,7
LIMIT 50;