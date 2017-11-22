--q38.sql--

with store_hot_cust as (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1200 and  1200 + 11),
catalog_hot_cust as (
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, date_dim, customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11),
web_hot_cust as (
    select distinct c_last_name, c_first_name, d_date
    from web_sales, date_dim, customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between  1200 and  1200 + 11),
hot_cust as (
    select sc.c_last_name, sc.c_first_name, sc.d_date
    from store_hot_cust sc, catalog_hot_cust cc, web_hot_cust wc
    where sc.c_last_name = cc.c_last_name AND
    sc.c_last_name = wc.c_last_name AND
    sc.c_first_name = cc.c_first_name AND
    sc.c_first_name = wc.c_first_name AND
    sc.d_date = cc.d_date AND
    sc.d_date = wc.d_date
)
 select count(*) from hot_cust
