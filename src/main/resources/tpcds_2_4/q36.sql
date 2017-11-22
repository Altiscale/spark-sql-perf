--q36.sql--

with x as
( select
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,(case when i_category is null then 1 else 0 end)+(case when i_class is null then 1 else 0 end) as lochierarchy,
   (case when (case when i_class is null then 1 else 0 end) = 0 then i_category end) as partition_2
 from
    store_sales, date_dim d1, item, store
 where
    d1.d_year = 2001
    and d1.d_date_sk = ss_sold_date_sk
    and i_item_sk  = ss_item_sk
    and s_store_sk  = ss_store_sk
    and s_state in ('TN','TN','TN','TN','TN','TN','TN','TN')
 group by i_category,i_class with rollup
)
select gross_margin, i_category, i_class, lochierarchy
   ,rank() over (
     partition by lochierarchy, partition_2
     order by gross_margin asc) as rank_within_parent
from x
order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
 limit 100

