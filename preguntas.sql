-- 1. Cuantos leads compró cada consumidor 
select distinct(comprador),count(*) from buyers group by comprador
-- 2. Cuantos leads creó cada productor 
select distinct(productor_id),count(*) from leads group by productor_id
-- 3. Cuanto tiempo promedio se tardó cada productor en producir un elemento 
select 
productor_id,
((max(fechahora_ingesta)-min(fechahora_ingesta))/count(*))
from leads
group by
productor_id
-- 4. Cuanto tiempo le lleva a todo el sistema terminar de producir y consumir 
select 
(max(c.fechahora_ingesta)-min(p.fechahora_ingesta))
from buyers as c inner join leads as p on (c.lead_id=p.id)
-- 5. Cuanto tiempo tarde en terminar el sistema en un modelo de alternancia 
select 
(max(c.fechahora_ingesta)-min(p.fechahora_ingesta))
from buyers as c inner join leads as p on (c.lead_id=p.id)