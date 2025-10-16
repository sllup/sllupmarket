with src as (
  select
    data,
    produto,
    sku,
    familia,
    sub_familia,
    cor,
    tam,
    marca,
    cod_cliente,
    razao_social,
    qtde,
    preco_unit,
    total_venda,
    total_custo,
    margem,
    documento_fiscal
  from staging.raw_vendas_achatado
),
casted as (
  select
    -- se vier DD/MM/YYYY, a API já converte para YYYY-MM-DD
    to_date(data, 'YYYY-MM-DD') as data,
    produto,
    sku,
    familia,
    sub_familia,
    cor,
    tam,
    marca,
    cod_cliente,
    razao_social,
    nullif(qtde,'')::numeric(18,4) as qtde,
    nullif(preco_unit,'')::numeric(18,4) as preco_unit,
    nullif(total_venda,'')::numeric(18,4) as total_venda,
    nullif(total_custo,'')::numeric(18,4) as total_custo,
    nullif(margem,'')::numeric(18,4) as margem,
    documento_fiscal
  from src
)
select * from casted;