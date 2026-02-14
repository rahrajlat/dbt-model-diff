select a.*,1 as is_test from {{ ref('int_customers') }} a 
