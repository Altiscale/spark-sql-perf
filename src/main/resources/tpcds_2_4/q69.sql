--q69.sql--

 select
    cd_gender, cd_marital_status, cd_education_status, count(*) cnt1,
    cd_purchase_estimate, count(*) cnt2, cd_credit_rating, count(*) cnt3
 from
    customer c 
    JOIN customer_address ca  ON
    (c.c_current_addr_sk = ca.ca_address_sk)
    JOIN customer_demographics ON
    (cd_demo_sk = c.c_current_cdemo_sk)
    LEFT SEMI JOIN (select ss_customer_sk from store_sales JOIN date_dim ON
                (ss_sold_date_sk = d_date_sk) 
                WHERE d_year = 2001 and
                d_moy between 4 and 4+2) ssdd
    ON (c.c_customer_sk = ssdd.ss_customer_sk)
    LEFT OUTER JOIN (select ws_bill_customer_sk from web_sales JOIN date_dim ON
                    (ws_sold_date_sk = d_date_sk) 
                    WHERE d_year = 2001 and
                    d_moy between 4 and 4+2) wsdd 
    ON (c.c_customer_sk = wsdd.ws_bill_customer_sk)
    LEFT OUTER JOIN (select cs_ship_customer_sk from catalog_sales JOIN date_dim ON
                    (cs_sold_date_sk = d_date_sk)
                    WHERE d_year = 2001 and
                    d_moy between 4 and 4+2) csdd
    ON c.c_customer_sk = csdd.cs_ship_customer_sk
 where
    ca_state in ('KY', 'GA', 'NM') and
    wsdd.ws_bill_customer_sk IS NULL and
    csdd.cs_ship_customer_sk IS NULL
 group by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 order by cd_gender, cd_marital_status, cd_education_status,
          cd_purchase_estimate, cd_credit_rating
 limit 100
            
