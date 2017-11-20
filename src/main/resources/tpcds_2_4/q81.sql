--q81.sql--

 with customer_total_return as
 (select
    cr_returning_customer_sk as ctr_customer_sk, ca_state as ctr_state,
        sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns, date_dim, customer_address
 where cr_returned_date_sk = d_date_sk
   and d_year = 2000
   and cr_returning_addr_sk = ca_address_sk
 group by cr_returning_customer_sk, ca_state )
 select
    c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name,
    ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,
    ca_gmt_offset,ca_location_type,ctr1.ctr_total_return
 from customer_total_return ctr1 JOIN customer ON
       ctr1.ctr_customer_sk = c_customer_sk
      JOIN customer_address ON
       ca_address_sk = c_current_addr_sk
      JOIN customer_total_return ctr2 ON
       ctr1.ctr_state = ctr2.ctr_state
 where ca_state = 'GA'
 group by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name,
    ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,
    ca_gmt_offset,ca_location_type,ctr1.ctr_total_return 
 having ctr1.ctr_total_return > avg(ctr2.ctr_total_return)*1.2
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 limit 100
            
