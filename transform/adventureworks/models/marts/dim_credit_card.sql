with salesorderheader as (
    select distinct creditcardid
    from {{ ref('salesorderheader') }}
    where creditcardid is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['salesorderheader.creditcardid']) }} as creditcard_key,
    salesorderheader.creditcardid,
    creditcard.cardtype
from salesorderheader
inner join {{ ref('creditcard') }} as creditcard
    on salesorderheader.creditcardid = creditcard.creditcardid
