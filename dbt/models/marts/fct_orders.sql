SELECT

    invoice_id,
    product_id,
    product_name,

    customer_id,
    country,

    quantity,
    unit_price,

    quantity * unit_price AS total_price,

    order_date

FROM {{ ref('stg_orders') }}