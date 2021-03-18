

##########################################################################################################################################
Descripcion: Este trigger se activa cuando se inserta (un nuevo resumen) un nuevo elemento en la tabla plan_vuelo_notam_trafico_resumen, obtiene la parte lista_notam (un vector),
recorre el vector e inserta en la tabla PIB_TIEMPO_REAL
					    
									     
/*--DE AQUI EN ADELANTE MUESTRA EL CONTROL AUTOMATICO PIB--*/
#####################################################################################################
--CHARLIE-----CONTROL AUTOMATICO PIB
select * from plan_vuelo_pib_tiempo_real
select * from aro_ais_historico_pib
insert into aro_ais_historico_pib (lista_notam, fecha_modificado) values ('ang', current_timestamp)
truncate aro_ais_historico_pib
alter SEQUENCE aro_ais_historico_pib_id_pib_historico_seq restart with 1;
		
			
DROP FUNCTION control_automatico_pib();
create or replace function control_automatico_pib() returns trigger as $control_automatico_pib$
	declare
		reg varchar;
		
		
		vector_notam_enpib text[];

	BEGIN
		
		vector_notam_enpib := (select ARRAY(SELECT distinct row(tn.id_notam_pib) FROM
		(select id_notam_pib, split_part(id_notam_pib,' ',3)as codigo from plan_vuelo_pib_tiempo_real
		where (id_notam_pib like '%NOTAMR%') OR (id_notam_pib like '%NOTAMC%')
		)as t1
		inner join
		plan_vuelo_pib_tiempo_real as tn
		on
		tn.id_notam_pib like '(' || t1.codigo ||'%'));
		
		foreach reg in array vector_notam_enpib
--		for reg in cur_notamy 
			LOOP
				if reg is null then
				else
					delete from plan_vuelo_pib_tiempo_real where id_notam_pib like reg;
					update plan_vuelo_notam_trafico_charly_new set es_pib = 'false'
					where idnotam like reg || '%';
					update plan_vuelo_notam_trafico_charly_repla set es_pib = 'false' 
					where idnotam like '%' || reg;
				end if;
				--raise notice 'Procesando %', reg.id_notam_pib; 
			END LOOP;
	
	--ACTUALIZAMOS EL DOCUMENTO DE REGISTRO PIB EN LA BASE DE DATOS, REGISTRANDO LA ULTIMA MODIFICACION
	insert into aro_ais_historico_pib (lista_notam, fecha_modificado) values ( documentacion_notam_pib(), current_timestamp);
	return NEW;
end;
$control_automatico_pib$ LANGUAGE 'plpgsql';

select control_automatico_pib();


CREATE OR REPLACE FUNCTION proteger_datos_pib() RETURNS TRIGGER AS $proteger_datos_pib$
  DECLARE
	vector text[];
	notam varchar;
	
	regx RECORD;

	cur_notam21 CURSOR FOR select id_notam_pib from plan_vuelo_pib_tiempo_real;
  
  BEGIN
	vector := (select string_to_array(new.resumen_lista,';'));
	
	  ----------------------------------------------- 
	for regx in cur_notam21
		LOOP
		--- si el elemento "no esta" en el vector_resumen_notam, entonces lo eliminamos
			if regx.id_notam_pib != ANY (vector::text[]) then
				delete from plan_vuelo_pib_tiempo_real where id_notam_pib like regx.id_notam_pib;

				update plan_vuelo_notam_trafico_charly_new set es_pib = 'false'
				where idnotam like regx.id_notam_pib || '%';

				update plan_vuelo_notam_trafico_charly_repla set es_pib = 'false' 
				where idnotam like regx.id_notam_pib || '%';
			end if;
			--raise notice 'Procesando %', reg.id_notam_pib; 
		END LOOP;
		----------------------------------------------- 
		--- Insertamos todo el vector resumen en la tabla pib_tiempo_real, si se repite no entra(repeticion de clave primaria), y si entra se guardara un registro de doucmento pib
		foreach notam in array vector
			loop
				if not existe_en_tabla_pib_tiempo_real(notam)
				then
					update plan_vuelo_notam_trafico_charly_new set es_pib=true where idnotam like notam ||'%';
					update plan_vuelo_notam_trafico_charly_repla set es_pib=true where idnotam like notam ||'%';

					insert into plan_vuelo_pib_tiempo_real (id_notam_pib,hora_actualizacion) values (notam,current_timestamp);	
				end if;

			end loop;
		--------------------------------------------------
  return NEW;
  END;
$proteger_datos_pib$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS proteger_datos_pib 
ON plan_vuelo_notam_trafico_resumen CASCADE  ;
CREATE TRIGGER proteger_datos_pib AFTER INSERT 
    ON plan_vuelo_notam_trafico_resumen 
	FOR EACH ROW EXECUTE PROCEDURE proteger_datos_pib();
	
						     

CREATE OR REPLACE FUNCTION existe_en_tabla_pib_tiempo_real(cadena varchar) RETURNS boolean AS $BODY$
  DECLARE
	respuesta_consulta text;
	resultado boolean;
  BEGIN
  	respuesta_consulta := (select id_notam_pib from plan_vuelo_pib_tiempo_real where id_notam_pib like cadena );
	
	if respuesta_consulta is null
	then 
		return false;
	else
	    return true;
	end if;
	
  END;
$BODY$ LANGUAGE plpgsql;


select not existe_en_tabla_pib_tiempo_real('(C2432/19' );

#####################################     EJEMPLO ################################################################

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
SELECT * from aro_ais_historico_pib;

select idnotam, es_pib
from (
	select idnotam,es_pib from plan_vuelo_notam_trafico_charly_new
	union 
	select idnotam,es_pib from plan_vuelo_notam_trafico_charly_repla
) as t1
where t1.es_pib=true
order by t1.idnotam desc




select * from plan_vuelo_notam_trafico_resumen where id_mensaje_resumen like 'ZPX0002180000/18032021';

insert into plan_vuelo_notam_trafico_resumen (	id_mensaje_resumen,aftn1,aftn2,resumen,resumen_lista,ingresado) values ('ZPX0002180000/18032021','GG SLLPYGZA','180000 SLLPYNYX','RESUMEN DIARIO DE NOTAM SERIE C VIGENTES AL 2103180000:
2019:2432 2467 2468 2470 2471 3098
2020:0105 1812 1950 2391 2395 2403 2405 2428 2429
2021:0102 0112 0113 0115 0119 0123 0124 0128 0152 0153 0173 0174
0182 0183 0184 0185 0186 0187 0188 0189 0190 0191 0192 0193
0194 0195 0196 0197 0201 0216 0233 0234 0235 0239 0244 0252
0255 0273 0278 0282 0283 0284 0285 0287 0290 0291 0297 0298
0302 0313 0315 0316 0317 0335 0341 0343 0347 0348 0363 0364
0366 0371 0372 0378 0379 0380 0381 0387 0389

','(C2432/19;(C2467/19;(C2468/19;(C2470/19;(C2471/19;(C3098/19;(C0105/20;(C1812/20;(C1950/20;(C2391/20;(C2395/20;(C2403/20;(C2405/20;(C2428/20;(C2429/20;(C0102/21;(C0112/21;(C0113/21;(C0115/21;(C0119/21;(C0123/21;(C0124/21;(C0128/21;(C0152/21;(C0153/21;(C0173/21;(C0174/21;(C0182/21;(C0183/21;(C0184/21;(C0185/21;(C0186/21;(C0187/21;(C0188/21;(C0189/21;(C0190/21;(C0191/21;(C0192/21;(C0193/21;(C0194/21;(C0195/21;(C0196/21;(C0197/21;(C0201/21;(C0216/21;(C0233/21;(C0234/21;(C0235/21;(C0239/21;(C0244/21;(C0252/21;(C0255/21;(C0273/21;(C0278/21;(C0282/21;(C0283/21;(C0284/21;(C0285/21;(C0287/21;(C0290/21;(C0291/21;(C0297/21;(C0298/21;(C0302/21;(C0313/21;(C0315/21;(C0316/21;(C0317/21;(C0335/21;(C0341/21;(C0343/21;(C0347/21;(C0348/21;(C0363/21;(C0364/21;(C0366/21;(C0371/21;(C0372/21;(C0378/21;(C0379/21;(C0380/21;(C0381/21;(C0387/21;(C0389/21',current_timestamp);

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

##########################################################################################################################################
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

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DE UN NOTAM DE REEMPLAZO

create or replace function charlie_buscar_from_notamr_to_notamr(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',1);
		var_notamr = substring(var_notamr,2);
		cadena := (
		select distinct t3.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_repla pac
			)as t3 
		where
		t3.var_idnotam like var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamr_to_notamr('(C9992/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM DE CANCELADO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function charlie_buscar_from_notamr_to_notamc(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',1);
		var_notamr = substring(var_notamr,2);
		
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_cancel pac
			)as t1 
		where
		t1.var_idnotam like var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamr_to_notamc('(C9994/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM DE CANCELADO A PARTIR DEL NOTAM NUEVO

create or replace function charlie_buscar_from_notamn_to_notamc(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamn = split_part(var_notamn,' ',1);
		var_notamn = substring(var_notamn,2);
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_cancel pac
			)as t1 
		where
		t1.var_idnotam like var_notamn || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamn_to_notamc('(C0229/21 NOTAMN');

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----HISTORIAL DE CHARLIE A PARTIR DEL NOTAM NUEVO

create or replace function charlie_historico_from_notamn(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(charlie_buscar_from_notamn_to_notamc(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(charlie_buscar_from_notamn_to_notamc(var_notamn) AS varchar),';');
		else
			if length(charlie_buscar_from_notamn_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(charlie_buscar_from_notamn_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(charlie_buscar_from_notamr_to_notamr(reemplazo)) > 0
				loop
					reemplazo=cast(charlie_buscar_from_notamr_to_notamr(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(charlie_buscar_from_notamr_to_notamc(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(charlie_buscar_from_notamr_to_notamc(reemplazo) AS varchar),';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select charlie_historico_from_notamn('(C0229/21 NOTAMN');--nuevo a cancelado
select charlie_historico_from_notamn('(C0248/21 NOTAMN');--nuevo a reemplazo
select charlie_historico_from_notamn('(C9990/21 NOTAMN');--nuevo a reemplazo a cancelado

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM CANCELADO AL NOTAM NUEVO--*/
##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM NUEVO A PARTIR DEL NOTAM DE CANCELADO

create or replace function charlie_buscar_from_notamc_to_notamn(var_notamc varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamc = split_part(var_notamc,' ',3);
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_new pac
			)as t1 
		where
		t1.var_idnotam like '%' || var_notamc || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamc_to_notamn('(C0230/21 NOTAMC C0229/21');

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM DE CANCELADO

create or replace function charlie_buscar_from_notamc_to_notamr(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',3);
		
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_repla pac
			)as t1 
		where
		t1.var_idnotam like '%' || var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamc_to_notamr('(C9994/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--CHARLIE-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function charlie_buscar_from_notamre_to_notamre(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',3);
		cadena := (
		select distinct t3.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_repla pac
			)as t3 
		where
		t3.var_idnotam like '%' || var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamre_to_notamre('(C9993/21 NOTAMR C9992/21');

##########################################################################################################################################



##########################################################################################################################################
----CHARLIE-----BUSCAR NOTAM NUEVO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function charlie_buscar_from_notamr_to_notamn(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamn = split_part(var_notamn,' ',3);
		cadena := (
		select distinct t2.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_charly_new pac
			)as t2
		where
		t2.var_idnotam like '%' || var_notamn || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select charlie_buscar_from_notamr_to_notamn('(C9991/21 NOTAMR C9990/21');

##########################################################################################################################################




##########################################################################################################################################
--CHARLIE-----HISTORIAL DE CHARLIE A PARTIR DEL NOTAM DE CANCELADO

create or replace function charlie_historico_from_notamc(var_notamn varchar)returns varchar as $$

	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(charlie_buscar_from_notamc_to_notamn(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(charlie_buscar_from_notamc_to_notamn(var_notamn) AS varchar),';');
		else
			if length(charlie_buscar_from_notamc_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(charlie_buscar_from_notamc_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(charlie_buscar_from_notamre_to_notamre(reemplazo)) > 0
				loop
					reemplazo=cast(charlie_buscar_from_notamre_to_notamre(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(charlie_buscar_from_notamr_to_notamn(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(charlie_buscar_from_notamr_to_notamn(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select charlie_historico_from_notamc('(C9995/21 NOTAMC C9994/21');
select charlie_historico_from_notamc('(C0230/21 NOTAMC C0229/21');

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM REEMPLAZADO AL NOTAM CANCELADO HACIA ADELANTE--*/
##########################################################################################################################################
--CHARLIE-----HISTORIAL DE CHARLIE A PARTIR DEL NOTA REEMPLAZADO AL NOTAM CANCELADO

create or replace function charlie_historico_from_notamr(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(charlie_buscar_from_notamr_to_notamc(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(charlie_buscar_from_notamr_to_notamc(var_notamn) AS varchar),';');
		else
			if length(charlie_buscar_from_notamr_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(charlie_buscar_from_notamr_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(charlie_buscar_from_notamr_to_notamr(reemplazo)) > 0
				loop
					reemplazo=cast(charlie_buscar_from_notamr_to_notamr(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(charlie_buscar_from_notamr_to_notamc(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(charlie_buscar_from_notamr_to_notamc(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select charlie_historico_from_notamr('(C9993/21 NOTAMR C9992/21');

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM REEMPLAZADO AL NOTAM NUEVO HACIA ATRAS--*/
##########################################################################################################################################
--CHARLIE-----HISTORIAL DE CHARLIE A PARTIR DEL NOTAM REEMPLAZADO AL NOTAM NUEVO

create or replace function charlie_historico_from_notamr2(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(charlie_buscar_from_notamr_to_notamn(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(charlie_buscar_from_notamr_to_notamn(var_notamn) AS varchar),';');
		else
			if length(charlie_buscar_from_notamc_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(charlie_buscar_from_notamc_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(charlie_buscar_from_notamre_to_notamre(reemplazo)) > 0
				loop
					reemplazo=cast(charlie_buscar_from_notamre_to_notamre(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(charlie_buscar_from_notamr_to_notamn(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador,CAST(charlie_buscar_from_notamr_to_notamn(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select charlie_historico_from_notamr('(C9993/21 NOTAMR C9992/21') || '' ||
charlie_historico_from_notamr2('(C9993/21 NOTAMR C9992/21')as columna

##########################################################################################################################################






/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM NUEVO AL NOTAM CANCELADO--*/

##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM NUEVO

create or replace function alpha_buscar_from_notamn_to_notamr(var_notamn varchar)returns varchar as $$
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
				 plan_vuelo_notam_trafico_alfa_repla pac
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

select alpha_buscar_from_notamn_to_notamr('(C9990/21 NOTAMN');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DE UN NOTAM DE REEMPLAZO

create or replace function alpha_buscar_from_notamr_to_notamr(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',1);
		var_notamr = substring(var_notamr,2);
		cadena := (
		select distinct t3.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_repla pac
			)as t3 
		where
		t3.var_idnotam like var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamr_to_notamr('(C9992/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE CANCELADO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function alpha_buscar_from_notamr_to_notamc(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',1);
		var_notamr = substring(var_notamr,2);
		
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_cancel pac
			)as t1 
		where
		t1.var_idnotam like var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamr_to_notamc('(C9994/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE CANCELADO A PARTIR DEL NOTAM NUEVO

create or replace function alpha_buscar_from_notamn_to_notamc(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamn = split_part(var_notamn,' ',1);
		var_notamn = substring(var_notamn,2);
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',3) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_cancel pac
			)as t1 
		where
		t1.var_idnotam like var_notamn || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamn_to_notamc('(C0229/21 NOTAMN');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----HISTORIAL DE CHARLIE A PARTIR DEL NOTAM NUEVO

create or replace function alpha_historico_from_notamn(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(alpha_buscar_from_notamn_to_notamc(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(alpha_buscar_from_notamn_to_notamc(var_notamn) AS varchar),';');
		else
			if length(alpha_buscar_from_notamn_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(alpha_buscar_from_notamn_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(alpha_buscar_from_notamr_to_notamr(reemplazo)) > 0
				loop
					reemplazo=cast(alpha_buscar_from_notamr_to_notamr(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(alpha_buscar_from_notamr_to_notamc(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(alpha_buscar_from_notamr_to_notamc(reemplazo) AS varchar),';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select alpha_historico_from_notamn('(C0229/21 NOTAMN');--nuevo a cancelado
select alpha_historico_from_notamn('(C0248/21 NOTAMN');--nuevo a reemplazo
select alpha_historico_from_notamn('(C9990/21 NOTAMN');--nuevo a reemplazo a cancelado

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM CANCELADO AL NOTAM NUEVO--*/
##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM NUEVO A PARTIR DEL NOTAM DE CANCELADO

create or replace function alpha_buscar_from_notamc_to_notamn(var_notamc varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamc = split_part(var_notamc,' ',3);
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_new pac
			)as t1 
		where
		t1.var_idnotam like '%' || var_notamc || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamc_to_notamn('(C0230/21 NOTAMC C0229/21');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM DE CANCELADO

create or replace function alpha_buscar_from_notamc_to_notamr(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',3);
		
		cadena := (
		select distinct t1.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_repla pac
			)as t1 
		where
		t1.var_idnotam like '%' || var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamc_to_notamr('(C9994/21 NOTAMR C9993/21');

##########################################################################################################################################



##########################################################################################################################################
--ALPHA-----BUSCAR NOTAM DE REEMPLAZO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function alpha_buscar_from_notamre_to_notamre(var_notamr varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamr = split_part(var_notamr,' ',3);
		cadena := (
		select distinct t3.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_repla pac
			)as t3 
		where
		t3.var_idnotam like '%' || var_notamr || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamre_to_notamre('(C9993/21 NOTAMR C9992/21');

##########################################################################################################################################



##########################################################################################################################################
----ALPHA-----BUSCAR NOTAM NUEVO A PARTIR DEL NOTAM DE REEMPLAZO

create or replace function alpha_buscar_from_notamr_to_notamn(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	begin
		var_notamn = split_part(var_notamn,' ',3);
		cadena := (
		select distinct t2.idnotam 
		from 
			(select 
				 distinct pac.idnotam, split_part(pac.idnotam,' ',1) as var_idnotam
			 from 
				 plan_vuelo_notam_trafico_alfa_new pac
			)as t2
		where
		t2.var_idnotam like '%' || var_notamn || '%'
		);
		if cadena is null 
		then 
			return '';
		else
			return cadena;
		end if;
	end
$$ language plpgsql;

select alpha_buscar_from_notamr_to_notamn('(C9991/21 NOTAMR C9990/21');

##########################################################################################################################################




##########################################################################################################################################
--ALPHA-----HISTORIAL DE ALPHA A PARTIR DEL NOTAM DE CANCELADO

create or replace function alpha_historico_from_notamc(var_notamn varchar)returns varchar as $$

	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(alpha_buscar_from_notamc_to_notamn(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(alpha_buscar_from_notamc_to_notamn(var_notamn) AS varchar),';');
		else
			if length(alpha_buscar_from_notamc_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(alpha_buscar_from_notamc_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(alpha_buscar_from_notamre_to_notamre(reemplazo)) > 0
				loop
					reemplazo=cast(alpha_buscar_from_notamre_to_notamre(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(alpha_buscar_from_notamr_to_notamn(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(alpha_buscar_from_notamr_to_notamn(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select charlie_historico_from_notamc('(C9995/21 NOTAMC C9994/21');
select charlie_historico_from_notamc('(C0230/21 NOTAMC C0229/21');

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM REEMPLAZADO AL NOTAM CANCELADO HACIA ADELANTE--*/
##########################################################################################################################################
--ALPHA-----HISTORIAL DE ALPHA A PARTIR DEL NOTA REEMPLAZADO AL NOTAM CANCELADO

create or replace function alpha_historico_from_notamr(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(alpha_buscar_from_notamr_to_notamc(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(alpha_buscar_from_notamr_to_notamc(var_notamn) AS varchar),';');
		else
			if length(alpha_buscar_from_notamr_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(alpha_buscar_from_notamr_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(alpha_buscar_from_notamr_to_notamr(reemplazo)) > 0
				loop
					reemplazo=cast(alpha_buscar_from_notamr_to_notamr(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(alpha_buscar_from_notamr_to_notamc(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador, CAST(alpha_buscar_from_notamr_to_notamc(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select alpha_historico_from_notamr('(C9993/21 NOTAMR C9992/21');

##########################################################################################################################################



/*--DE AQUI EN ADELANTE SE REALIZA PARA QUE BUSQUE DEL NOTAM REEMPLAZADO AL NOTAM NUEVO HACIA ATRAS--*/
##########################################################################################################################################
--ALPHA-----HISTORIAL DE ALPHA A PARTIR DEL NOTAM REEMPLAZADO AL NOTAM NUEVO

create or replace function alpha_historico_from_notamr2(var_notamn varchar)returns varchar as $$
	declare cadena varchar;
	declare acumulador varchar;
	declare reemplazo varchar;
	begin
		acumulador='';
		if length(alpha_buscar_from_notamr_to_notamn(var_notamn) )> 0 
		then
			acumulador=concat(var_notamn,';',CAST(alpha_buscar_from_notamr_to_notamn(var_notamn) AS varchar),';');
		else
			if length(alpha_buscar_from_notamc_to_notamr(var_notamn)) > 0
			then
				reemplazo=cast(alpha_buscar_from_notamc_to_notamr(var_notamn) as varchar);
				acumulador=concat(acumulador, reemplazo, ';');

				
				while length(alpha_buscar_from_notamre_to_notamre(reemplazo)) > 0
				loop
					reemplazo=cast(alpha_buscar_from_notamre_to_notamre(reemplazo) as varchar);
					acumulador=concat(acumulador, reemplazo, ';');
				end loop;		
				
				if length(alpha_buscar_from_notamr_to_notamn(reemplazo)) > 0
				then
					acumulador=concat(var_notamn,';',acumulador,CAST(alpha_buscar_from_notamr_to_notamn(reemplazo) AS varchar), ';');
				else
					acumulador=concat(var_notamn,';',acumulador);
				end if;
			end if;
			acumulador=concat(acumulador);
		end if;
		return acumulador;
	end$$
language plpgsql;

select alpha_historico_from_notamr('(C9993/21 NOTAMR C9992/21') || '' ||
alpha_historico_from_notamr2('(C9993/21 NOTAMR C9992/21')as columna

##########################################################################################################################################
									  
									     




/*--DE AQUI EN ADELANTE MUESTRA LA UNION DEL ID_NOTAM_PIB Y HORA_ACTUALIZACION--*/
#####################################################################################################
--CHARLIE-----CONCATENACION DE CADENAS ID_NOTAM_PIB DE LA TABLA PIB_TIEMPO_REAL Y HORA_ACTUALIZACION


drop function documentacion_notam_pib();
create or replace function documentacion_notam_pib() returns varchar as 
$BODY$
	DECLARE
	reg RECORD;
	cur_notams CURSOR FOR SELECT (id_notam_pib,hora_actualizacion)as notam from
	plan_vuelo_pib_tiempo_real order by id_notam_pib;
	cadena varchar;
	BEGIN
		cadena:='';
		for reg in cur_notams loop 
			cadena := cadena || reg.notam || ';';
		end loop;
		return cadena;
	end;
$BODY$
LANGUAGE 'plpgsql';
select documentacion_notam_pib();
#####################################################################################################
