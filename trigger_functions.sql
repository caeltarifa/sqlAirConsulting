##########################################################################################################################################
Descripcion: Este trigger se activa cuando se inserta (un nuevo resumen) un nuevo elemento en la tabla plan_vuelo_notam_trafico_resumen, obtiene la parte lista_notam (un vector),
recorre el vector e inserta en la tabla PIB_TIEMPO_REAL

CREATE OR REPLACE FUNCTION proteger_datos_pib() RETURNS TRIGGER AS $proteger_datos_pib$
  DECLARE
  vector text[];
  notam varchar;
  BEGIN
	  truncate plan_vuelo_pib_tiempo_real;
	  update plan_vuelo_notam_trafico_charly_new set es_pib=false;
	  update plan_vuelo_notam_trafico_charly_repla set es_pib=false;

	  vector := (select string_to_array(new.resumen_lista,';'));
	  foreach notam in array vector
	  loop

		update plan_vuelo_notam_trafico_charly_new set es_pib=true where idnotam like notam ||'%';
		update plan_vuelo_notam_trafico_charly_repla set es_pib=true where idnotam like notam ||'%';


		insert into plan_vuelo_pib_tiempo_real (id_notam_pib,hora_actualizacion) values (notam,current_timestamp);	
		
	  end loop;
	  
  return NEW;
  END;
$proteger_datos_pib$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS proteger_datos_pib 
ON plan_vuelo_notam_trafico_resumen CASCADE  ;
CREATE TRIGGER proteger_datos_pib AFTER INSERT 
    ON plan_vuelo_notam_trafico_resumen 
	FOR EACH ROW EXECUTE PROCEDURE proteger_datos_pib();
		

#################################  EJEMPLO  ##############################################
delete from plan_vuelo_pib_tiempo_real where id_notam_pib like '(CYYYY/19';
delete from plan_vuelo_notam_trafico_resumen where id_mensaje_resumen like 'ZPX0001100000/XXXXX';

insert into plan_vuelo_notam_trafico_resumen (	id_mensaje_resumen,aftn1,aftn2,resumen,resumen_lista,ingresado) values ('ZPX0001100000/XXXXX','GG SLLPYGZA','100000 SLLPYNYX','RESUMEN DIARIO DE NOTAM SERIE C VIGENTES AL 2103100000:
2019:2432 2467 2468 2470 2471 3098
2020:0105 1812 1950 2311 2332 2391 2395 2403 2405 2427 2428 2429
2021:0037 0102 0112 0113 0115 0119 0123 0124 0128 0152 0153 0173
0174 0178 0182 0183 0184 0185 0186 0187 0188 0189 0190 0191
0192 0193 0194 0195 0196 0197 0201 0216 0226 0227 0233 0234
0235 0239 0244 0252 0255 0257 0259 0273 0278 0282 0283 0284
0285 0287 0290 0291 0293 0297 0298 0302 0312 0313 0314 0315
0316 0317 0320 0335 0336 0338 0339 0340

','(CYYYY/19;(C2467/19;(C2468/19;(C2470/19;(C2471/19;(C3098/19;(C0105/20;(C1812/20;(C1950/20;(C2311/20;(C2332/20;(C2391/20;(C2395/20;(C2403/20;(C2405/20;(C2427/20;(C2428/20;(C2429/20;(C0037/21;(C0102/21;(C0112/21;(C0113/21;(C0115/21;(C0119/21;(C0123/21;(C0124/21;(C0128/21;(C0152/21;(C0153/21;(C0173/21;(C0174/21;(C0178/21;(C0182/21;(C0183/21;(C0184/21;(C0185/21;(C0186/21;(C0187/21;(C0188/21;(C0189/21;(C0190/21;(C0191/21;(C0192/21;(C0193/21;(C0194/21;(C0195/21;(C0196/21;(C0197/21;(C0201/21;(C0216/21;(C0226/21;(C0227/21;(C0233/21;(C0234/21;(C0235/21;(C0239/21;(C0244/21;(C0252/21;(C0255/21;(C0257/21;(C0259/21;(C0273/21;(C0278/21;(C0282/21;(C0283/21;(C0284/21;(C0285/21;(C0287/21;(C0290/21;(C0291/21;(C0293/21;(C0297/21;(C0298/21;(C0302/21;(C0312/21;(C0313/21;(C0314/21;(C0315/21;(C0316/21;(C0317/21;(C0320/21;(C0335/21;(C0336/21;(C0338/21;(C0339/21;(C0340/21',current_timestamp);

SELECT * from plan_vuelo_pib_tiempo_real;

select idnotam, es_pib
from (
	select idnotam,es_pib from plan_vuelo_notam_trafico_charly_new
	union 
	select idnotam,es_pib from plan_vuelo_notam_trafico_charly_repla
) as t1
where t1.es_pib=true
order by t1.idnotam desc

##########################################################################################################################################

					    
					    
					    
/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM NUEVO AL NOTAM CANCELADO--*/

#####################################################################################################
--CHARLIE-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM NUEVO

create or replace function charlie_buscar_from_notamn_to_notamr(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamn = split_part(var_notamn,' ',1);
		var_notamn = substring(var_notamn,2);
		cadena := (
		select distinct t2.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_repla pac
			)as t2
		where
		t2.var_idnotam like var_notamn || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamn_to_notamr('(C9990/21 NOTAMN');

#####################################################################################################
