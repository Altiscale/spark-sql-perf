--q33.sql--

 with ss as (
   select i1.i_manufact_id, sum(ss_ext_sales_price) total_sales
     from store_sales JOIN date_dim ON
       ss_sold_date_sk = d_date_sk
     JOIN item i1 ON 
       ss_item_sk = i1.i_item_sk
     JOIN customer_address ON
       ss_addr_sk = ca_address_sk
     LEFT SEMI JOIN item i2 ON
       (i1.i_manufact_id = i2.i_manufact_id AND
       i2.i_category = 'Electronics')
     WHERE
       d_year = 1998
       and d_moy = 5
       and ca_gmt_offset = -5
    GROUP BY i1.i_manufact_id), 

  cs as (SELECT i_manufact_id, sum(cs_ext_sales_price) total_sales
    FROM catalog_sales JOIN date_dim ON
      cs_sold_date_sk = d_date_sk
    JOIN item i1 ON
      cs_item_sk = i1.i_item_sk
    JOIN customer_address ON 
      cs_bill_addr_sk = ca_address_sk
    LEFT SEMI JOIN item i2 ON
      (i1.i_manufact_id = i2.i_manufact_id AND
       i2.i_category = 'Electronics')
    WHERE d_year = 1998
      and d_moy = 5
      and ca_gmt_offset = -5
      group by i1.i_manufact_id),

 ws as ( select i1.i_manufact_id,sum(ws_ext_sales_price) total_sales
   FROM web_sales JOIN date_dim ON 
     ws_sold_date_sk = d_date_sk
   JOIN customer_address ON
     ws_bill_addr_sk = ca_address_sk
   JOIN item i1 ON
     ws_item_sk = i1.i_item_sk
   LEFT SEMI JOIN item i2 ON
     (i1.i_manufact_id = i2.i_manufact_id AND
      i2.i_category = 'Electronics')
   WHERE d_year = 1998
      and d_moy = 5
      and ca_gmt_offset = -5
      group by i1.i_manufact_id)

 select i_manufact_id ,sum(total_sales) total_sales
 from  (select * from ss
        union all
        select * from cs
        union all
        select * from ws) tmp1
 group by i_manufact_id
 order by total_sales
limit 100
            
