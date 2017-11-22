--q86.sql--

with x as (
 select sum(ws_net_paid) as total_sum, i_category, i_class,
  (case when i_category is null then 1 else 0 end)+(case when i_class is null then 1 else 0 end) as lochierarchy,
  (case when (case when i_class is null then 1 else 0 end) = 0 then i_category end) as partition_2
 from
    web_sales, date_dim d1, item
 where
    d1.d_month_seq between 1200 and 1200+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by i_category,i_class with rollup
)
select total_sum, i_category, i_class, lochierarchy,
  rank() over (
     partition by lochierarchy, partition_2
     order by total_sum asc) as rank_within_parent
from x
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category end,
   rank_within_parent
 limit 100
            
