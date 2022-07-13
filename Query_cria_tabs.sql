create table bases_gb.tbvendamesano
as select cast(DATA_VENDA as STRING FORMAT 'MMYYYY') as MesAno, sum(QTD_VENDA) as TotalVendas
from  (select * from `projetogb-356021.bases_gb.base2017` 
      union distinct select * from `projetogb-356021.bases_gb.base2018` 
      union distinct select * from `projetogb-356021.bases_gb.base2019`)
group by MesAno


create table bases_gb.tbvendamarcaproduto
as select ID_MARCA, MARCA, ID_LINHA, LINHA, sum(QTD_VENDA) as TotalVendas
from  (select * from `projetogb-356021.bases_gb.base2017` 
      union distinct select * from `projetogb-356021.bases_gb.base2018` 
      union distinct select * from `projetogb-356021.bases_gb.base2019`)
group by ID_MARCA, MARCA, ID_LINHA, LINHA



create table  bases_gb.tbvendamarcamesano
as select MARCA, cast(DATA_VENDA as STRING FORMAT 'MMYYYY') as MesAno, sum(QTD_VENDA) as TotalVendas
from  (select * from `projetogb-356021.bases_gb.base2017` 
      union distinct select * from `projetogb-356021.bases_gb.base2018` 
      union distinct select * from `projetogb-356021.bases_gb.base2019`)
group by MesAno,MARCA
order by MARCA

create table  bases_gb.tbvendalinhamesano
as select LINHA, cast(DATA_VENDA as STRING FORMAT 'MMYYYY') as MesAno, sum(QTD_VENDA) as TotalVendas
from  (select * from `projetogb-356021.bases_gb.base2017` 
      union distinct select * from `projetogb-356021.bases_gb.base2018` 
      union distinct select * from `projetogb-356021.bases_gb.base2019`)
group by MesAno,linha
order by LINHA
