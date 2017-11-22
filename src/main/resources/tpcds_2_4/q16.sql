--q16.sql--

 select
   count(distinct cs1.cs_order_number) as order_count,
   sum(cs1.cs_ext_ship_cost) as `total shipping cost`,
   sum(cs1.cs_net_profit) as `total net profit`
 from
   catalog_sales cs1, date_dim, customer_address, call_center,
   catalog_sales cs2
   left join catalog_returns cr1 on ( cs1.cs_order_number = cr1.cr_order_number )
 where
   d_date between cast ('2002-02-01' as date) and (cast('2002-02-01' as date) + interval '60' day)
 and cs1.cs_ship_date_sk = d_date_sk
 and cs1.cs_ship_addr_sk = ca_address_sk
 and ca_state = 'GA'
 and cs1.cs_call_center_sk = cc_call_center_sk
 and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County', 'Williamson County') 
 and cs1.cs_order_number = cs2.cs_order_number
 and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
 and cr1.cr_order_number = null
 order by order_count
 limit 100
            
