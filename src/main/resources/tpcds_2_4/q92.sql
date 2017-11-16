--q92.sql--

with avg_amount as
     (
       SELECT ws_item_sk as av_ws_item_sk, 1.3 * avg(ws_ext_discount_amt) as av
       FROM web_sales, date_dim
       WHERE 
         d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
         and d_date_sk = ws_sold_date_sk
       GROUP BY ws_item_sk
     )

 select sum(ws_ext_discount_amt) as `Excess Discount Amount`
 from web_sales, item, date_dim, avg_amount
 where i_manufact_id = 350
 and i_item_sk = ws_item_sk
 and d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
 and d_date_sk = ws_sold_date_sk
 and ws_ext_discount_amt > avg_amount.av
 and i_item_sk = av_ws_item_sk
 order by `Excess Discount Amount`
 limit 100
            
