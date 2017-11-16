--q32.sql--

with avgdiscount as
(
          select 1.3 * avg(cs_ext_discount_amt) av
          from catalog_sales, date_dim
          where d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
           and d_date_sk = cs_sold_date_sk)
 select sum(cs_ext_discount_amt) as `excess discount amount`
 from
    catalog_sales, item, date_dim, avgdiscount
 where
   i_manufact_id = 977
   and i_item_sk = cs_item_sk
   and d_date between cast ('2000-01-27' as date) and (cast('2000-01-27' as date) + interval '90' day)
   and d_date_sk = cs_sold_date_sk
   and cs_ext_discount_amt > avgdiscount.av
limit 100
            
