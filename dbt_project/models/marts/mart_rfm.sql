with base as (
  select cod_cliente,
         max(data) as ultima_compra,
         count(*)  as freq,
         sum(coalesce(total_venda,0)) as valor
  from {{ ref('fato_venda') }}
  group by 1
),
rfm as (
  select
    cod_cliente,
    ultima_compra,
    freq,
    valor,
    ntile(5) over (order by ultima_compra desc) as r_score,
    ntile(5) over (order by freq desc)          as f_score,
    ntile(5) over (order by valor desc)         as m_score
  from base
)
select * from rfm;