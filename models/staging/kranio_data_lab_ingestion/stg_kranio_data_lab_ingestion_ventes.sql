with 

source as (

    select * from {{ source('kranio_data_lab_ingestion','ventes') }}

),

renamed as (

    select
        date,
        CONCAT(id_produit,"-",id_client)
        id_produit,
        id_client,
        prix_unitaire,
        ingestion_date,
        execution_date,
        ingestion_year,
        ingestion_month,
        ingestion_day

    from source

)

select * from renamed
