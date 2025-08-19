SELECT 
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} AS order_item_key,
    l_orderkey as order_key,
    l_partkey as part_key,
    l_linenumber as line_number,
    l_quantity as quanitity,
    l_extendedprice as extended_price,
    l_discount as discount_percentage,
    l_tax as rate_tax
FROM {{source('tpch','lineitem')}}