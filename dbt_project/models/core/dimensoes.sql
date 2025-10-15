-- dim_cliente (exemplo minimalista)
with base as (
  select cod_cliente, max(razao_social) as razao_social
  from {{ ref('stg_vendas') }}
  where cod_cliente is not null
  group by 1
)
select * from base;