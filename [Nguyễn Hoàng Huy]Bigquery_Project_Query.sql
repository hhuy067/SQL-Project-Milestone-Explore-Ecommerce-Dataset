-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL


 select
    distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month,
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions,
    round(sum(totals.totalTransactionRevenue / 1000000),2) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0101' and '0331'
group by month
order by month

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL


 select
  distinct trafficSource.source,
  sum(totals.visits) as total_visit,
  sum(totals.bounces) as total_no_of_bounces,
  sum(totals.bounces) / sum(totals.visits) * 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0701' and '0731'
group by source
order by total_visit DESC


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL

with week as (
  select
"week" as time_type,
    FORMAT_DATE('%Y%W' , PARSE_DATE("%Y%m%d",date)) as time,
 trafficSource.source,
  sum(totals.totalTransactionRevenue) as revenue,
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0601' and '0630'
group by time_type,time,source),
month as	(
	select
"month" as time_type,
    FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as time,
 trafficSource.source,
  sum(totals.totalTransactionRevenue) as revenue,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0601' and '0630'
group by time_type,time,source)

select * 
from week
UNION ALL
select *
from month	

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with pur as(
SELECT 
    distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month,
     sum(totals.pageviews) / count (distinct fullVisitorId) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0601' and '0731' and totals.transactions >= 1
group by month)
,
non as(
SELECT 
    distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month,
     sum(totals.pageviews) / count (distinct fullVisitorId) as avg_pageviews_non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0601' and '0731' and totals.transactions is null
group by month)

select 
    pur.month, avg_pageviews_purchase, avg_pageviews_non_purchase
from pur
join 
non
on pur.month = non.month 


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

 select
   distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month,
    sum(totals.transactions) /  count (distinct fullVisitorId) as avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0701' and '0731' and totals.transactions >= 1
group by month

-- Query 06: Average amount of money spent per session
#standardSQL

 select
   distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month,
   sum(totals.totalTransactionRevenue) / sum(totals.visits) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
Where _table_suffix between '0701' and '0731' and totals.transactions is not null
group by month 

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

select
 product.v2ProductName as other_purchased_products,
 sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, unnest(hits) hits , unnest (product) as product
Where _table_suffix between '0701' and '0731' and product.productRevenue is not null
group by other_purchased_products

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with npv as (
select
 distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month, 
 count (hits.eCommerceAction.action_type) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, unnest(hits) hits 
Where _table_suffix between '0101' and '0331'and  hits.eCommerceAction.action_type = '2' 
group by month),
na as (
select
 distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month, 
 count (hits.eCommerceAction.action_type) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, unnest(hits) hits 
Where _table_suffix between '0101' and '0331'and  hits.eCommerceAction.action_type = '3' 
group by month ),
np as (
select
 distinct FORMAT_DATE('%Y%m' , PARSE_DATE("%Y%m%d",date)) as month, 
 count (hits.eCommerceAction.action_type) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, unnest(hits) hits 
Where _table_suffix between '0101' and '0331'and  hits.eCommerceAction.action_type = '6' 
group by month )
select 
 npv.month,
 num_product_view,
 num_addtocart,
 num_purchase,
 (num_addtocart / num_product_view * 100) as add_to_cart_rate,
 (num_purchase / num_product_view * 100) as purchase_rate
from npv 
join 
na 
on npv.month = na.month
join 
np
on npv.month = np.month
order by npv.month