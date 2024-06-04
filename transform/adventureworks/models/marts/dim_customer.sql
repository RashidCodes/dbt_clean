select
    {{ dbt_utils.generate_surrogate_key(['customer.customerid']) }} as customer_key,
    customer.customerid,
    person.businessentityid as personbusinessentityid,
    concat(coalesce(person.firstname, ''), ' ', coalesce(person.middlename, ''), ' ', coalesce(person.lastname, '')) as fullname,
    store.businessentityid as storebusinessentityid,
    store.storename
from {{ ref('customer') }} as customer
left join {{ ref('person') }} as person
    on customer.personid = person.businessentityid
left join {{ ref('store') }} as store
    on customer.storeid = store.businessentityid
