SELECT
  all_conv_in_dates.timezone as 'Timezone'
  , CONVERT_TZ(all_conv_in_dates.receive_time,'GMT',all_conv_in_dates.timezone) as 'Conversion Timestamp'
  , CONVERT_TZ(all_conv_in_dates.entry_time,'GMT',all_conv_in_dates.timezone) as 'Click Timestamp'  
  , if( k.keyword IS NULL, '', k.keyword ) as 'Keyword'
  , if( k.keyword_type IS NULL, '', k.keyword_type ) as 'Keyword Type'
  , if( pg.publisher_group_name IS NULL, '', pg.publisher_group_name ) as 'Group'
  , if( pc.campaign_name IS NULL, '', pc.campaign_name ) as 'Campaign'
  , CASE  WHEN pa.publisher_id=4 THEN 'Google'
      WHEN pa.publisher_id=5 THEN 'Yahoo'
      WHEN pa.publisher_id=6 THEN 'Bing'
      WHEN pa.publisher_id IS NULL THEN if( search_term IS NULL, '', replace( referring_domain, 'www.', '') )
      ELSE ''
  END as 'Publisher'

  , CASE
        WHEN MIN( tr.receive_time ) IS NOT NULL OR MIN( all_conv_in_dates.receive_time ) > DATE_ADD( MIN( all_conv_in_dates.entry_time ), INTERVAL all_conv_in_dates.conversion_window DAY ) OR pg.publisher_group_name IS NULL OR pg.publisher_group_name ='' THEN ''
        ELSE 1
    END as 'Conversions'

  , CASE
        WHEN MIN( tr.receive_time ) IS NOT NULL OR MIN( all_conv_in_dates.receive_time ) > DATE_ADD( MIN( all_conv_in_dates.entry_time ), INTERVAL all_conv_in_dates.conversion_window DAY ) OR all_conv_in_dates.total = 0 OR pg.publisher_group_name IS NULL OR pg.publisher_group_name ='' THEN ''
        ELSE all_conv_in_dates.total
  END as 'Revenue'

    , MIN(all_conv_in_dates.affiliation) as 'Affiliation'
    , MIN(all_conv_in_dates.tax) as 'Tax'
    , MIN(all_conv_in_dates.shipping) as 'Shipping'
    , MIN(all_conv_in_dates.city) as 'City'
    , MIN(all_conv_in_dates.state) as 'State'
    , MIN(all_conv_in_dates.country) as 'Country'
    , IFNULL( MIN(all_conv_in_dates.product_name), '' ) as 'Product Name'
    , IFNULL( MIN(all_conv_in_dates.category), '' ) as 'Category'
    , MIN(all_conv_in_dates.price) as 'Price'
    , MIN(all_conv_in_dates.quantity) as 'Quantity'
    , MIN(all_conv_in_dates.order_id) as 'Order Id'
    , MIN(all_conv_in_dates.order_type) as 'Order Type'
    , MIN(all_conv_in_dates.total) as 'Order Total'
    , MIN(all_conv_in_dates.uuid) as 'Uuid'
    , MIN(all_conv_in_dates.Entry_URL) as 'Entry Url'
        
    , IFNULL( DATE_FORMAT( MIN( CONVERT_TZ(tr.receive_time,'GMT',all_conv_in_dates.timezone) ), '%m/%d/%y %r' ), '' ) as 'QA: first_conv'
    
    , CASE
        WHEN MIN( tr.receive_time ) IS NOT NULL AND all_conv_in_dates.total=0 
            THEN 'Ignored: Dup Conversion'
        WHEN MIN( tr.receive_time ) IS NOT NULL AND all_conv_in_dates.total>0 
            THEN CONCAT( 'Ignored ($',all_conv_in_dates.total,'): Dup Conversion' )
        WHEN MIN( all_conv_in_dates.receive_time ) > DATE_ADD( MIN( all_conv_in_dates.entry_time ), INTERVAL all_conv_in_dates.conversion_window DAY ) AND all_conv_in_dates.total=0 
            THEN CONCAT( 'Ignored: Exceeds ', all_conv_in_dates.conversion_window, 'd Window' )
        WHEN MIN( all_conv_in_dates.receive_time ) > DATE_ADD( MIN( all_conv_in_dates.entry_time ), INTERVAL all_conv_in_dates.conversion_window DAY ) AND all_conv_in_dates.total>0 
            THEN CONCAT( 'Ignored ($',all_conv_in_dates.total,'): Exceeds ', all_conv_in_dates.conversion_window, 'd Window'  )
        ELSE ''
    END as 'QA: comment'
    
FROM
  (
  SELECT
      cl.time_zone as 'timezone' 
    , cl.conversion_window as 'conversion_window' 
    , receive_time as 'receive_time'
    , entry_time as 'entry_time'
    , IFNULL(entry_url, '') as 'Entry_URL'
        , uuid as 'uuid'
        , keyword_instance_id as 'kwid'
        , publisher_group_id as 'pgid'
        , total as 'total'
        , order_id as 'order_id'
        , order_type as 'order_type'
      , affiliation as 'affiliation'
      , tax as 'tax'
      , shipping as 'shipping'
      , city as 'city'
      , state as 'state'
      , country as 'country'
      , product_name as 'product_name'
      , category as 'category'
      , price as 'price'
      , quantity as 'quantity'
      , COUNT(1) as 'consolidated_entries'

  FROM
      marin_olap_staging.tracker_data_16865
        , marin.clients cl

  WHERE
        -- locate client to pull converion window
        cl.client_id = 16865
    AND action_id = 2
    AND receive_time >= CONVERT_TZ(2015-01-01,cl.time_zone,'GMT')
    AND receive_time < DATE_ADD( CONVERT_TZ(2015-12-31,cl.time_zone,'GMT'), INTERVAL 1 DAY )   

    -- regardless of the date period we're extracting conversions, we will 
    -- always ignore conversion events that happen AFER the conversion window
    AND receive_time <= DATE_ADD( entry_time, INTERVAL 30 DAY )
    
    -- comment this out for all orders, or leave in for PPC orders
    AND publisher_group_id > 0

  GROUP BY
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21

  ) as all_conv_in_dates

    -- now want to process de-dupping logic. We do this by looking for 
    -- other action=2 that have the same order_id OR receive_time
    LEFT JOIN marin_olap_staging.tracker_data_16865 tr ON 
        (all_conv_in_dates.order_id IS NOT NULL OR all_conv_in_dates.order_id <> '')

        -- only look for action=2
        AND tr.action_id = 2

        -- only look for same user's activity
        AND tr.uuid = all_conv_in_dates.uuid

        -- only process conversions with same inbound click time
        AND tr.entry_time = all_conv_in_dates.entry_time

        -- ensure same conversion type
        AND tr.order_type = all_conv_in_dates.order_type

        -- look for an earlier order so we can establish as dup
        AND tr.receive_time < all_conv_in_dates.receive_time

        -- look for an earlier order so we can establish as dup
        AND tr.order_id = all_conv_in_dates.order_id

  LEFT JOIN marin.keyword_instances k on (all_conv_in_dates.kwid = k.id)
  LEFT JOIN marin.publisher_groups pg on (all_conv_in_dates.pgid = pg.publisher_group_id)
  LEFT JOIN marin.publisher_campaigns pc on (pg.publisher_campaign_id = pc.publisher_campaign_id)
  LEFT JOIN marin.publisher_client_accounts pca on (pc.client_account_id = pca.client_account_id)
  LEFT JOIN marin.publisher_accounts pa on (pca.publisher_account_id = pa.publisher_account_id)


  GROUP BY
    1,2,3,4,5,6,7,8

    -- order the results by user and then desceending by receive_time
    ORDER BY
        all_conv_in_dates.receive_time DESC
        , all_conv_in_dates.entry_time DESC;          