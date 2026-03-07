SELECT

    InvoiceNo   AS invoice_id,
    StockCode   AS product_id,
    Description AS product_name,

    Quantity    AS quantity,
    UnitPrice   AS unit_price,

    CustomerID  AS customer_id,
    Country     AS country,

    InvoiceDate AS order_date

FROM raw.orders