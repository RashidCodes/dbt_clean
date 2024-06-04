with salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
    from {{ ref('salesorderheader') }}
),

salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('salesorderdetail') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sh.salesorderid', 'sd.salesorderdetailid']) }} as sales_key,
    {{ dbt_utils.generate_surrogate_key(['sd.productid']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['sh.customerid']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['sh.creditcardid']) }} as creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['sh.shiptoaddressid']) }} as ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['sh.order_status']) }} as order_status_key,
    sh.orderdate,
    sd.salesorderid,
    sd.salesorderdetailid,
    sd.unitprice,
    sd.orderqty,
    sd.revenue
from salesorderdetail as sd
inner join salesorderheader as sh
    on sd.salesorderid = sh.salesorderid
