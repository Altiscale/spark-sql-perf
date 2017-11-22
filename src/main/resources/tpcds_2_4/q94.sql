--q94.sql--

 select
    count(distinct ws1.ws_order_number) as order_count
   ,sum(ws1.ws_ext_ship_cost) as `total shipping cost`
   ,sum(ws1.ws_net_profit) as `total net profit`
 from
    web_sales ws1, date_dim, customer_address, web_site,
    web_sales ws2   
    left join web_returns wr1 on ( ws1.ws_order_number = wr1.wr_order_number )
 where
     d_date between cast('1999-02-01' as date) and
            (cast('1999-02-01' as date) + interval '60' day)
 and ws1.ws_ship_date_sk = d_date_sk
 and ws1.ws_ship_addr_sk = ca_address_sk
 and ca_state = 'IL'
 and ws1.ws_web_site_sk = web_site_sk
 and web_company_name = 'pri'
 and ws1.ws_order_number = ws2.ws_order_number
 and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
 and wr1.wr_order_number = null
 order by order_count
 limit 100
            
