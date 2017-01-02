SELECT DISTINCT parsed.last_click_time AS 'Click Time',
  parsed.uuid as 'UUID',
  c.client_id as 'Client ID',
  c.client_name AS 'Client Name',
  p.publisher AS 'Publisher',
  pca.alias AS 'Account',
  pc.campaign_name AS 'Campaign',
  pg.publisher_group_name AS 'Group', 
  parsed.mkwid as 'Tracking Value',
  ki.id as 'Marin Keyword ID',
  COALESCE(ki.keyword, parsed.keyword) AS 'Keyword', 
  COALESCE(ki.keyword_type, CASE 
    WHEN parsed.match_type IN ('b', 'bb') THEN 
      'BROAD'
    WHEN parsed.match_type IN ('e', 'be') THEN 
      'EXACT'
    WHEN parsed.match_type IN ('p', 'bp') THEN 
      'PHRASE'
    END) AS 'Match Type',
  IF(pcr_ext.creative_id, pcr_ext.ext_id, pcr.ext_id) as 'Publisher Creative ID',
  IF(pcr_ext.creative_id, pcr_ext.creative_id, pcr.creative_id) as 'Marin Creative ID',
  IF(pcr_ext.creative_id, pcr_ext.headline, pcr.headline) AS 'Creative Headline',
  IF(pcr_ext.creative_id, pcr_ext.description1, pcr.description1) AS 'Creative Description 1',
  IF(pcr_ext.creative_id, pcr_ext.description2, pcr.description2) AS 'Creative Description 2',
  IF(pcr_ext.creative_id, pcr_ext.display_url, pcr.display_url) AS 'Creative Display URL',
  pl.placement_url as 'Placement',
  pt.product_target_name as 'Product Group',
  parsed.product_name AS 'Product Name',
  parsed.category AS 'Category'
FROM 
(
  SELECT clicks.day,
    clicks.last_click_time,
    clients.client_id as parent_client_id,
    clients.customer_id,
    clicks.uuid,
    clicks.product_name,
    clicks.category,
    clicks.keyword_instance_id,
    clicks.creative_id,
    clicks.publisher_group_id,
    case when click.entry_url regexp CONCAT(mkwid.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, CONCAT(mkwid.param_name, '='), -1), mkwid.param_stop, 1), coalesce(device_delimiter.setting_value, '|'), 1)
    when click.entry_url regexp CONCAT(mkwid.param_name, mkwid.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, CONCAT(mkwid.param_name, mkwid.param_stop), -1), mkwid.param_stop, 1), coalesce(device_delimiter.setting_value, '|'), 1)
    end as mkwid,
    case when click.entry_url regexp CONCAT(pcrid.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, CONCAT(pcrid.param_name, '='), -1), pcrid.param_stop, 1)
    when click.entry_url regexp CONCAT(pcrid.param_name, pcrid.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, CONCAT(pcrid.param_name, pcrid.param_stop), -1), pcrid.param_stop, 1)
    end as pcrid,
    case when click.entry_url REGEXP CONCAT(pkw.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(marin.urldecode(click.entry_url), concat(pkw.param_name, '='), -1), pkw.param_stop, 1)
    when click.entry_url REGEXP CONCAT(pkw.param_name, pkw.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(marin.urldecode(click.entry_url), concat(pkw.param_name, pkw.param_stop), -1), pkw.param_stop, 1)
    end as keyword,
    case when click.entry_url REGEXP CONCAT(pmt.param_name, '=') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, concat(pmt.param_name, '='), -1), pmt.param_stop, 1)
    when click.entry_url REGEXP CONCAT(pmt.param_name, pmt.param_stop) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, concat(pmt.param_name, pmt.param_stop), -1), pmt.param_stop, 1)
    end as match_type,
    case when click.entry_url REGEXP CONCAT(pdv.param_name, pdv.param_separator) then
      SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, concat(pdv.param_name, pdv.param_separator), -1), pdv.param_stop, 1)
    end as device,
    case when click.entry_url REGEXP CONCAT(device_delimiter.setting_value, 'd') then
      SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(click.entry_url, concat(mkwid.param_name, '='), -1), mkwid.param_stop, 1), concat(device_delimiter.setting_value, 'd'), -1)
    end as device_combined,
    COALESCE(IF(click.query REGEXP 'currency=', SUBSTRING_INDEX(SUBSTRING_INDEX(click.query, 'currency=', -1), '&', 1), NULL), clients.currency) as currency,
    click.entry_url
  FROM 
  (
    SELECT   
      DATE(CONVERT_TZ(clicks.receive_time, 'GMT', clients.time_zone)) AS DAY,
      clicks.uuid,
      clicks.product_name,
      clicks.category,
      clicks.keyword_instance_id,
      clicks.creative_id,
      clicks.publisher_group_id,
      MAX(CONVERT_TZ(clicks.receive_time, 'GMT', clients.time_zone)) AS last_click_time
    FROM marin_olap_staging.tracker_data_51536 clicks 
      JOIN marin.clients ON marin.clients.client_id = 51536
      JOIN marin.client_keyword_params mkwid on clients.client_id = mkwid.client_id and mkwid.search_order = 0 
    WHERE clicks.action_id = 1
      AND clicks.entry_url LIKE CONCAT('%', mkwid.param_name, '%')
      AND clicks.receive_time BETWEEN CONVERT_TZ('$MSTART', clients.time_zone, 'GMT')
        AND CONVERT_TZ('$MEND',  clients.time_zone, 'GMT') + INTERVAL 1 DAY
    GROUP BY
      1,2,3,4,5,6,7
  ) clicks
    JOIN marin.clients ON client_id = 51536
    JOIN marin_olap_staging.tracker_data_51536 click ON clicks.uuid = click.uuid AND clicks.last_click_time = CONVERT_TZ(click.receive_time, 'GMT', clients.time_zone) AND click.action_id = 1
    JOIN marin.client_keyword_params mkwid on clients.client_id = mkwid.client_id and mkwid.search_order = 0 
    JOIN marin.client_creative_params pcrid on clients.client_id = pcrid.client_id and pcrid.search_order = 0 
    LEFT JOIN marin.application_settings device_delimiter ON clients.client_id = device_delimiter.client_id AND device_delimiter.setting = 'TRACKING_ID_DELIM'
    LEFT JOIN marin.client_keyword_text_params pkw on clients.client_id = pkw.client_id and pkw.search_order = 0 
    LEFT JOIN marin.client_match_type_params pmt on clients.client_id = pmt.client_id and pmt.search_order = 0
    LEFT JOIN marin.client_tracking_params pdv on clients.client_id = pdv.client_id and pdv.search_order = 0 
  WHERE click.entry_url LIKE CONCAT('%', mkwid.param_name, '%')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) parsed
  LEFT JOIN marin.tracking_values tv ON tv.value = parsed.mkwid and tv.client_id in (select client_id from marin.clients where customer_id = parsed.customer_id and status = 'ACTIVE')
  LEFT JOIN marin.publisher_creatives as pcr_ext on pcr_ext.ext_id = parsed.pcrid and parsed.pcrid NOT REGEXP '{.+}' and parsed.pcrid NOT REGEXP '%7B.+%7D'
  LEFT JOIN marin.publisher_creative_old_ext_ids as pcr_old_ext_ids on pcr_old_ext_ids.old_ext_id = parsed.pcrid and parsed.pcrid NOT REGEXP '{.+}' and parsed.pcrid NOT REGEXP '%7B.+%7D'
  LEFT JOIN marin.publisher_creatives pcr_from_old_ext ON pcr_from_old_ext.creative_id = pcr_old_ext_ids.publisher_creative_id
  LEFT JOIN marin.publisher_creatives pcr ON pcr.creative_id = IF(parsed.creative_id > 0, parsed.creative_id, IF(tv.trackable_type = 'PublisherCreative', tv.trackable_id, NULL))  
  LEFT JOIN marin.keyword_instances ki ON ki.id = IF(parsed.keyword_instance_id > 0, parsed.keyword_instance_id, IF(tv.trackable_type = 'KeywordInstance', tv.trackable_id, 'NOMATCH')) -- and ki.publisher_group_id = COALESCE(pcr_ext.publisher_group_id, pcr.publisher_group_id)
  LEFT JOIN marin.product_targets pt ON pt.product_target_id = IF(tv.trackable_type = 'ProductTarget', tv.trackable_id, NULL)
  LEFT JOIN marin.placements pl ON pl.placement_id = IF(tv.trackable_type = 'Placement', tv.trackable_id, NULL)
  LEFT JOIN marin.publisher_groups pg ON pg.publisher_group_id = COALESCE(ki.publisher_group_id, pcr_ext.publisher_group_id, pcr_from_old_ext.publisher_group_id, pcr.publisher_group_id, pt.publisher_group_id, pl.publisher_group_id)
  LEFT JOIN marin.publisher_campaigns pc ON pg.publisher_campaign_id = pc.publisher_campaign_id
  LEFT JOIN marin.publisher_client_accounts pca ON pc.client_account_id = pca.client_account_id
  LEFT JOIN marin.publisher_accounts pa ON pca.publisher_account_id = pa.publisher_account_id
  LEFT JOIN marin.clients c ON pa.client_id = c.client_id and c.customer_id = parsed.customer_id and c.status = 'ACTIVE'
  LEFT JOIN marin.publishers p ON pa.publisher_id = p.publisher_id
WHERE (ki.id IS NULL OR ki.publisher_group_id = COALESCE(pcr_ext.publisher_group_id, pcr_from_old_ext.publisher_group_id, pcr.publisher_group_id))
AND c.client_id = $MCLIENT
ORDER BY 1;