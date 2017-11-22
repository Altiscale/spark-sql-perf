--q6.sql--

with category_average as 
             (SELECT avg(i_current_price) as av, i_category as ca_i_category FROM item
                 group by i_category),
date_constraint as (SELECT distinct (d_month_seq) as dc_seq FROM date_dim 
        WHERE d_year = 2001 AND d_moy = 1)
SELECT state, cnt FROM (
 SELECT a.ca_state state, count(*) cnt
 FROM
    customer_address a, customer c, store_sales s, date_dim d, item i,
    category_average ca
    left join date_constraint dc
 WHERE a.ca_address_sk = c.c_current_addr_sk
     AND c.c_customer_sk = s.ss_customer_sk
     AND s.ss_sold_date_sk = d.d_date_sk
     AND s.ss_item_sk = i.i_item_sk
     AND d.d_month_seq = dc.dc_seq
     AND i.i_current_price > 1.2 * ca.av
     AND ca.ca_i_category = i.i_category
 GROUP BY a.ca_state
) x
WHERE cnt >= 10
ORDER BY cnt LIMIT 100
            
