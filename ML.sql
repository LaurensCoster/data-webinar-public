#1 % data preview
select 
  * 
from `data-webinar.ga4_webinar.events_*`
TABLESAMPLE SYSTEM (1 percent);

#data statistics
select 
  count(distinct user_pseudo_id) as count_distinct_users,
  count(event_timestamp) as count_events
from `data-webinar.ga4_webinar.events_*`;

#create view with returingn users -- labeling users
CREATE OR REPLACE VIEW data-webinar.ga4_webinar.returningusers AS (
  WITH firstlasttouch AS (
    SELECT
      user_pseudo_id,
      MIN(event_timestamp) AS user_first_engagement,
      MAX(event_timestamp) AS user_last_engagement
    FROM
      `data-webinar.ga4_webinar.events_*`
    WHERE event_name="user_engagement"
    GROUP BY
      user_pseudo_id

  )
  SELECT
    user_pseudo_id,
    user_first_engagement,
    user_last_engagement,
    EXTRACT(MONTH from TIMESTAMP_MICROS(user_first_engagement)) as month,
    EXTRACT(DAYOFYEAR from TIMESTAMP_MICROS(user_first_engagement)) as julianday,
    EXTRACT(DAYOFWEEK from TIMESTAMP_MICROS(user_first_engagement)) as dayofweek,

    #add 24 hr to user's first touch
    (user_first_engagement + 86400000000) AS ts_24hr_after_first_engagement,

#churned = 1 if last_touch within 24 hr of app installation, else 0
IF (user_last_engagement < (user_first_engagement + 86400000000),
    1,
    0 ) AS churned,

#bounced = 1 if last_touch within 10 min, else 0
IF (user_last_engagement <= (user_first_engagement + 600000000),
    1,
    0 ) AS bounced,
  FROM
    firstlasttouch
  GROUP BY
    1,2,3
    );

#view preview'
SELECT 
  * 
FROM 
  data-webinar.ga4_webinar.returningusers
LIMIT 100;

#churning bunced statistics 
SELECT
    bounced,
    churned, 
    COUNT(churned) as count_users
FROM
    data-webinar.ga4_webinar.returningusers
GROUP BY 1,2
ORDER BY bounced;

#user demographics view
CREATE OR REPLACE VIEW data-webinar.ga4_webinar.user_demographics AS (

  WITH first_values AS (
      SELECT
          user_pseudo_id,
          geo.country as country,
          device.operating_system as operating_system,
          device.language as language,
          ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS row_num
      FROM `data-webinar.ga4_webinar.events_*`
      WHERE event_name="user_engagement"
      )
  SELECT * EXCEPT (row_num)
  FROM first_values
  WHERE row_num = 1
  );

#user demographics preview 
SELECT
  *
FROM
  data-webinar.ga4_webinar.user_demographics
LIMIT 10;

# user behavioral data stats 
SELECT
    event_name,
    COUNT(event_name) as event_count
FROM
    `data-webinar.ga4_webinar.events_*`
GROUP BY 1
ORDER BY
   event_count DESC;

#pivoted user behavioral data
CREATE OR REPLACE VIEW data-webinar.ga4_webinar.user_aggregate_behavior AS (
WITH
  events_first24hr AS (
    #select user data only from first 24 hr of using the app
    SELECT
      e.*
    FROM
      `data-webinar.ga4_webinar.events_*` e
    JOIN
      data-webinar.ga4_webinar.returningusers r
    ON
      e.user_pseudo_id = r.user_pseudo_id
    WHERE
      e.event_timestamp <= r.ts_24hr_after_first_engagement
    )
SELECT
  user_pseudo_id,
  SUM(IF(event_name = 'user_engagement', 1, 0)) AS cnt_user_engagement,
  SUM(IF(event_name = 'page_view', 1, 0)) AS cnt_page_view,
  SUM(IF(event_name = 'scroll', 1, 0)) AS cnt_scroll,
  SUM(IF(event_name = 'view_item', 1, 0)) AS cnt_view_item,
  SUM(IF(event_name = 'session_start', 1, 0)) AS cnt_session_start,
  SUM(IF(event_name = 'first_visit', 1, 0)) AS cnt_first_visit,
  SUM(IF(event_name = 'view_promotion', 1, 0)) AS cnt_view_promotion,
  SUM(IF(event_name = 'add_to_cart', 1, 0)) AS cnt_add_to_cart,
  SUM(IF(event_name = 'begin_checkout', 1, 0)) AS cnt_begin_checkout,
  SUM(IF(event_name = 'select_item', 1, 0)) AS cnt_select_item,
  SUM(IF(event_name = 'view_search_results', 1, 0)) AS cnt_view_search_results,
  SUM(IF(event_name = 'add_shipping_info', 1, 0)) AS cnt_add_shipping_info,
  SUM(IF(event_name = 'add_payment_info', 1, 0)) AS cnt_add_payment_info,
  SUM(IF(event_name = 'select_promotion', 1, 0)) AS cnt_select_promotion,
  SUM(IF(event_name = 'purchase', 1, 0)) AS cnt_purchase,
  SUM(IF(event_name = 'click', 1, 0)) AS cnt_click,
  SUM(IF(event_name = 'view_item_list', 1, 0)) AS cnt_view_item_list
FROM
  events_first24hr
GROUP BY
  1
  );

#preview user aggregate behavior 
SELECT
  *
FROM
  data-webinar.ga4_webinar.user_aggregate_behavior
LIMIT 100;


#build training set
CREATE OR REPLACE VIEW data-webinar.ga4_webinar.train AS ( 
  SELECT
    dem.*,
    IFNULL(beh.cnt_user_engagement, 0) AS user_engagement_count,
    IFNULL(beh.cnt_page_view, 0) AS page_view_count,
    IFNULL(beh.cnt_scroll, 0) AS scroll_count,
    IFNULL(beh.cnt_view_item, 0) AS view_item_count,
    IFNULL(beh.cnt_session_start, 0) AS session_start_count,
    IFNULL(beh.cnt_first_visit, 0) AS first_visit_count,
    IFNULL(beh.cnt_view_promotion, 0) AS view_promotion_count,
    IFNULL(beh.cnt_add_to_cart, 0) AS add_to_cart_count,
    IFNULL(beh.cnt_begin_checkout, 0) AS begin_checkout_count,
    IFNULL(beh.cnt_select_item, 0) AS select_item_count,
    IFNULL(beh.cnt_view_search_results, 0) AS view_search_results_count,
    IFNULL(beh.cnt_add_shipping_info, 0) AS add_shipping_info_count,
    IFNULL(beh.cnt_add_payment_info, 0) AS add_payment_info_count,
    IFNULL(beh.cnt_select_promotion, 0) AS select_promotion_count,
    IFNULL(beh.cnt_purchase, 0) AS purchase_count,
    IFNULL(beh.cnt_click, 0) AS click_count,
    IFNULL(beh.cnt_view_item_list, 0) AS view_item_list_count,
    ret.user_first_engagement,
    ret.month,
    ret.julianday,
    ret.dayofweek,
    ret.churned
  FROM
    data-webinar.ga4_webinar.returningusers ret
  LEFT OUTER JOIN
    data-webinar.ga4_webinar.user_demographics dem
  ON 
    ret.user_pseudo_id = dem.user_pseudo_id
  LEFT OUTER JOIN 
    data-webinar.ga4_webinar.user_aggregate_behavior beh
  ON
    ret.user_pseudo_id = beh.user_pseudo_id
  WHERE ret.bounced = 0
  );

#check training set
SELECT
  *
FROM
  data-webinar.ga4_webinar.train
LIMIT 10;


#create churn ML model 
CREATE OR REPLACE MODEL `data-webinar.ga4_webinar.churn_logreg`
OPTIONS(
  MODEL_TYPE="LOGISTIC_REG", #BOOSTED_TREE_CLASSIFIER
  INPUT_LABEL_COLS=["churned"]
) AS
SELECT
  *
FROM
  `data-webinar.ga4_webinar.train`;

#evalueate model
SELECT
  *
FROM
  ML.EVALUATE(MODEL `data-webinar.ga4_webinar.churn_logreg`);

#predict!
SELECT
  user_pseudo_id,
  churned,
  predicted_churned,
  predicted_churned_probs[OFFSET(0)].prob as probability_churned
FROM
  ML.PREDICT(MODEL `data-webinar.ga4_webinar.churn_logreg`,
  (SELECT * FROM `data-webinar.ga4_webinar.train`)) #can be replaced with a test dataset
where 
  churned = 0 and predicted_churned = 1;

