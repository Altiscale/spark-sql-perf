
--q14a.sql--

with x_store_items as 
    (select iss.i_brand_id brand_id, iss.i_class_id class_id, iss.i_category_id category_id
     from store_sales, item iss, date_dim d1
     where ss_item_sk = iss.i_item_sk
                    and ss_sold_date_sk = d1.d_date_sk
       and d1.d_year between 1999 AND 1999 + 2),
x_catalog_items as        
    ( select ics.i_brand_id brand_id, ics.i_class_id class_id, ics.i_category_id category_id
     from catalog_sales, item ics, date_dim d2
     where cs_item_sk = ics.i_item_sk
       and cs_sold_date_sk = d2.d_date_sk
       and d2.d_year between 1999 AND 1999 + 2),
x_web_items as
    ( select iws.i_brand_id brand_id, iws.i_class_id class_id, iws.i_category_id category_id
     from web_sales, item iws, date_dim d3
     where ws_item_sk = iws.i_item_sk
       and ws_sold_date_sk = d3.d_date_sk
       and d3.d_year between 1999 AND 1999 + 2),
cross_items as
 (select i_item_sk ss_item_sk
  from item, x_store_items, x_catalog_items, x_web_items 
  where
  x_store_items.brand_id = x_catalog_items.brand_id AND
  x_store_items.brand_id = x_web_items.brand_id AND
  x_store_items.class_id = x_catalog_items.class_id AND
  x_store_items.class_id = x_web_items.class_id AND
  x_store_items.category_id = x_catalog_items.category_id AND
  x_store_items.category_id = x_catalog_items.category_id AND
  i_brand_id = x_store_items.brand_id AND
  i_class_id = x_store_items.class_id AND
  i_category_id = x_store_items.category_id
),
 x_sales as
 (
     select ss_quantity quantity, ss_list_price list_price
     from store_sales, date_dim
     where ss_sold_date_sk = d_date_sk
       and d_year between 1999 and 2001
   union all
     select cs_quantity quantity, cs_list_price list_price
     from catalog_sales, date_dim
     where cs_sold_date_sk = d_date_sk
       and d_year between 1999 and 1999 + 2
   union all
     select ws_quantity quantity, ws_list_price list_price
     from web_sales, date_dim
     where ws_sold_date_sk = d_date_sk
       and d_year between 1999 and 1999 + 2),
avg_sales as
 (select avg(quantity*list_price) average_sales from x_sales),
xx as (
     select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
     from store_sales, item, date_dim, cross_items
     left join avg_sales
     where store_sales.ss_item_sk = cross_items.ss_item_sk 
       and store_sales.ss_item_sk = i_item_sk
       and ss_sold_date_sk = d_date_sk
       and d_year = 1999+2
       and d_moy = 11
     group by i_brand_id,i_class_id,i_category_id, avg_sales.average_sales
     having sum(ss_quantity*ss_list_price) > avg_sales.average_sales
   union all
     select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
     from catalog_sales, item, date_dim, cross_items
     left join avg_sales
     where catalog_sales.cs_item_sk = cross_items.ss_item_sk 
       and catalog_sales.cs_item_sk = i_item_sk
       and cs_sold_date_sk = d_date_sk
       and d_year = 1999+2
       and d_moy = 11
     group by i_brand_id,i_class_id,i_category_id, avg_sales.average_sales
     having sum(cs_quantity*cs_list_price) > avg_sales.average_sales
   union all
     select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
     from web_sales, item, date_dim, cross_items
     left join avg_sales
     where web_sales.ws_item_sk = cross_items.ss_item_sk 
       and web_sales.ws_item_sk = i_item_sk
       and ws_sold_date_sk = d_date_sk
       and d_year = 1999+2
       and d_moy = 11
     group by i_brand_id,i_class_id,i_category_id, avg_sales.average_sales
     having sum(ws_quantity*ws_list_price) > avg_sales.average_sales
 )
select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from xx
 group by channel, i_brand_id,i_class_id,i_category_id with rollup
 order by channel,i_brand_id,i_class_id,i_category_id
 limit 100
            
