select
    product_id,
    category_name, 
    name_length,
    description_length,
    photos_quantity,
    weight_g,
    length_cm,
    height_cm,
    width_cm
from {{ ref('int_products_enriched') }}