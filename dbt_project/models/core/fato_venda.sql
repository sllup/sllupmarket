select
  gen_random_uuid() as id_fato_venda,
  data,
  cod_cliente,
  sku,
  tam,
  qtde,
  preco_unit,
  total_venda,
  total_custo,
  margem,
  documento_fiscal,
  now() as carga_dt
from {{ ref('stg_vendas') }}
where data is not null and sku is not null and cod_cliente is not null;
