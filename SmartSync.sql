SELECT cc.publisher_campaign_id as child_camp, 
cc.child_groups as child_group_count, 
pc.publisher_campaign_id as parent_camp, 
pc.parent_groups as parent_group_count,
cc.child_groups - pc.parent_groups as Differential
FROM 
(SELECT parent_id,publisher_campaign_id, count(*) as child_groups 
FROM publisher_groups pg 
JOIN smart_sync_pairs ssp 
ON pg.publisher_campaign_id = ssp.child_id
WHERE publisher_campaign_id IN 
(SELECT child_id 
FROM smart_sync_pairs 
WHERE vo_type = 1 
AND client_id = 17166)
AND publisher_group_status != 'DELETED'

GROUP BY 1) cc 

JOIN

(SELECT publisher_campaign_id, count(*) as parent_groups 
FROM publisher_groups pg 
WHERE publisher_campaign_id IN 
(SELECT parent_id 
FROM smart_sync_pairs 
WHERE vo_type = 1 
AND client_id = 17166)
AND publisher_group_status != 'DELETED'

GROUP BY 1) pc 

ON cc.parent_id=pc.publisher_campaign_id 
WHERE pc.publisher_campaign_id in 
(SELECT parent_id FROM smart_sync_pairs WHERE vo_type = 1 
AND client_id = 17166) 
HAVING cc.child_groups != pc.parent_groups 
order by Differential
LIMIT 50
;


SELECT publisher_campaign_id, count(*) as child_groups 
FROM publisher_groups pg 
WHERE publisher_campaign_id IN 
(SELECT child_id 
FROM smart_sync_pairs 
WHERE vo_type = 1 
AND client_id = 51568)
AND publisher_group_status != 'DELETED'

GROUP BY 1
;

Account	Campaign	Group	Ad Type	App Type	App Name	Headline	Description line 1	Description line 2	Landing page	Device Preference
Google - 04__SEM_GG_IOS_refENG	GG_NB_SRH_it_ENGVAR_gXX_cNX_ai_ex	ENGTUR_Turkish learning apps	Mobile App Install	App Store	829587759	Turkish Learning App	Learn Turkish with Babbel's app.	Try Turkish in app & online!	https://itunes.apple.com/app/learn-spanish-english-french/id829587759	All
https://wiki.marinsw.net/w/SS:Scripts/getCampaignsForClient.sh
BASH to FILE
for i in $(cat notinstock.txt); do echo $i; runonstagclient 45419 "select ptd.publisher_group_name,ptd.publisher_campaign_name,ptd.client_account_id,mpt.product_target_name,product_target_dim_id,pt.creation_date,CASE WHEN conversion_type_id = 1052042 THEN sum(conversions) END Nl_lead, CASE WHEN conversion_type_id = 954136 THEN sum(conversions) END Lead, CASE WHEN conversion_type_id = 772583 THEN sum(conversions) END Order, sum(conversions),sum(ad_revenue), sum(impressions),sum(publisher_clicks),sum(publisher_cost) from product_target_fact_28100_45419 ptf join product_target_dim_28100_45419 ptd USING(product_target_dim_id) join marin.product_targets mpt USING(product_target_id) where mpt.product_target_name like '%$i%' and ptf.conversion_type_id in (954136,1052042,772583) and time_id in (select time_id from time_by_day_epoch where the_date between '2016-01-01' and NOW()) ;"; done >45419_metrics_sum.txt 

Non bash to FILE
runonstagclient 45419 "select ptd.publisher_group_name,ptd.publisher_campaign_name,ptd.client_account_id,mpt.product_target_name,mpt.creation_date,product_target_dim_id,CASE WHEN conversion_type_id = 1052042 THEN sum(conversions) END Nl_lead,CASE WHEN conversion_type_id = 954136 THEN sum(conversions) END Lead,CASE WHEN conversion_type_id = 772583 THEN sum(conversions) END Ord, sum(conversions),sum(ad_revenue), sum(impressions),sum(publisher_clicks),sum(publisher_cost) from product_target_fact_28100_45419 ptf join product_target_dim_28100_45419 ptd USING(product_target_dim_id) join marin.product_targets mpt USING(product_target_id) where mpt.product_target_name like 'All > Item ID:4003743822522' and ptf.conversion_type_id in (954136,1052042,772583) and time_id in (select time_id from time_by_day_epoch where the_date between '2016-01-01' and NOW());"


for i in $(cat notinstock.txt); do runonstagclient 45419 "select ptd.publisher_group_name,ptd.publisher_campaign_name,ptd.client_account_id,mpt.product_target_name,mpt.creation_date,product_target_dim_id,CASE WHEN conversion_type_id = 1052042 THEN sum(conversions) END Nl_lead,CASE WHEN conversion_type_id = 954136 THEN sum(conversions) END Lead,CASE WHEN conversion_type_id = 772583 THEN sum(conversions) END Ord, sum(conversions),sum(ad_revenue), sum(impressions),sum(publisher_clicks),sum(publisher_cost) from product_target_fact_28100_45419 ptf join product_target_dim_28100_45419 ptd USING(product_target_dim_id) join marin.product_targets mpt USING(product_target_id) where ptf.conversion_type_id in (954136,1052042,772583) and time_id in (select time_id from time_by_day_epoch where the_date between '2016-01-01' and NOW()) and mpt.product_target_name like '%4048672097597%' group by 4;"; done > 45419_products_all.txt