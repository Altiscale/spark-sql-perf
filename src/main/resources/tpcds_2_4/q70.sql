--q70.sql--

with
tmp1 as
(select s_state as t_s_state,
                   rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
         from store_sales, store, date_dim
         where  d_month_seq between 1200 and 1200+11
                and d_date_sk = ss_sold_date_sk
                and s_store_sk  = ss_store_sk
         group by s_state),
x as (
 select
    sum(ss_net_profit) as total_sum, s_state, s_county
   ,(case when s_state is null then 1 else 0 end)+(case when s_county is null then 1 else 0 end) as lochierarchy,
   (case when (case when s_county is null then 1 else 0 end) = 0 then s_state end) as partition_2
 from
    store_sales, date_dim d1, store, tmp1
 where
    d1.d_month_seq between 1200 and 1200+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state = tmp1.t_s_state
 and tmp1.ranking <= 5
 group by s_state,s_county with rollup
)
select total_sum, s_state, s_county, lochierarchy, partition_2
   ,rank() over (
         partition by lochierarchy, partition_2
         order by total_sum desc) as rank_within_parent
from x
order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent
 limit 100
            
