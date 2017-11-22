--q44.sql--

with avg_profit as 
(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 4
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk),
V1 as (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
         left join avg_profit
                 where ss_store_sk = 4
                 group by ss_item_sk, rank_col
                 having avg(ss_net_profit) > 0.9*avg_profit.rank_col),
V11 as (select item_sk,rank() over (order by rank_col asc) rnk
           from V1),
asceding as
(select * 
     from V11
     where rnk  < 11),
V21 as (select item_sk,rank() over (order by rank_col desc) rnk
           from V1),
descending as
(select * from V21 where rnk < 11)
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
 from
 asceding, descending,
 item i1, item i2
 where asceding.rnk = descending.rnk
   and i1.i_item_sk=asceding.item_sk
   and i2.i_item_sk=descending.item_sk
 order by asceding.rnk
 limit 100
            
