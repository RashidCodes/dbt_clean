select
    {{ dbt_utils.generate_surrogate_key(['address.addressid']) }} as address_key,
    address.addressid,
    address.city as city_name,
    stateprovince.name as state_name,
    countryregion.name as country_name
from {{ ref('address') }} as address
left join {{ ref('stateprovince') }} as stateprovince
    on address.stateprovinceid = stateprovince.stateprovinceid
left join {{ ref('countryregion') }} as countryregion
    on stateprovince.countryregioncode = countryregion.countryregioncode
