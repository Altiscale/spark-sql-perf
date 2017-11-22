----------q14b.sql--

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
     where ss_sold_date_sk = d_date_sk and d_year between 1999 and 1999 + 2
   union all
     select cs_quantity quantity, cs_list_price list_price
     from catalog_sales, date_dim
     where cs_sold_date_sk = d_date_sk and d_year between 1999 and 1999 + 2
   union all
     select ws_quantity quantity, ws_list_price list_price
     from web_sales, date_dim
     where ws_sold_date_sk = d_date_sk and d_year between 1999 and 1999 + 2),
avg_sales as
 (select avg(quantity*list_price) average_sales from x_sales),
date_constraint_last_year as (select d_week_seq as dc_seq
                                      from date_dim
				      where d_year = 1999 and d_moy = 12 and d_dom = 11),
date_constraint_this_year as (select d_week_seq as dc_seq
                                      from date_dim
				      where d_year = 1999 + 1 and d_moy = 12 and d_dom = 11),
this_year as (
     select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
     from store_sales, item, date_dim, cross_items, date_constraint_this_year dc
     left join avg_sales
     where store_sales.ss_item_sk = cross_items.ss_item_sk 
       and store_sales.ss_item_sk = i_item_sk
       and ss_sold_date_sk = d_date_sk
       and d_week_seq = dc.dc_seq
     group by i_brand_id,i_class_id,i_category_id, avg_sales.average_sales
     having sum(ss_quantity*ss_list_price) > avg_sales.average_sales),
last_year as (     
     select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
     from store_sales, item, date_dim, cross_items, date_constraint_last_year dc
     left join avg_sales
     where store_sales.ss_item_sk = cross_items.ss_item_sk 
       and store_sales.ss_item_sk = i_item_sk
       and ss_sold_date_sk = d_date_sk
       and d_week_seq = dc.dc_seq
     group by i_brand_id,i_class_id,i_category_id, avg_sales.average_sales
     having sum(ss_quantity*ss_list_price) > avg_sales.average_sales)
 select * from
 this_year, last_year
 where this_year.i_brand_id= last_year.i_brand_id
   and this_year.i_class_id = last_year.i_class_id
   and this_year.i_category_id = last_year.i_category_id
 order by this_year.channel, this_year.i_brand_id, this_year.i_class_id, this_year.i_category_id
 limit 100
