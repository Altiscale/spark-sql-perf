--q1.sql--

WITH customer_total_return AS
   (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk,
           sum(sr_return_amt) AS ctr_total_return
    FROM store_returns, date_dim
    WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk)
 SELECT c.c_customer_id
   FROM customer_total_return ctr1 INNER JOIN store s ON
     ctr1.ctr_store_sk = s.s_store_sk
   INNER JOIN customer c ON
     ctr1.ctr_customer_sk = c.c_customer_sk
   INNER JOIN customer_total_return ctr2 ON
     ctr1.ctr_store_sk = ctr2.ctr_store_sk
   WHERE s.s_state = 'TN'
   GROUP BY c.c_customer_id, ctr1.ctr_total_return
   HAVING (ctr1.ctr_total_return > avg(ctr2.ctr_total_return)*1.2)
   ORDER BY c.c_customer_id LIMIT 100;
