WITH customer AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'customer') }}

),

nation AS (

  SELECT * 
  
  FROM {{ source('samples.tpch', 'nation') }}

),

customer_nation AS (

  SELECT 
    customer.c_custkey AS c_custkey,
    customer.c_name AS c_name,
    customer.c_address AS c_address,
    customer.c_nationkey AS c_nationkey,
    customer.c_phone AS c_phone,
    customer.c_acctbal AS c_acctbal,
    customer.c_mktsegment AS c_mktsegment,
    customer.c_comment AS c_comment,
    nation.n_name AS n_name,
    nation.n_regionkey AS n_regionkey,
    nation.n_comment AS n_comment
  
  FROM customer
  INNER JOIN nation
     ON customer.c_nationkey = nation.n_nationkey

)

SELECT *

FROM customer_nation
