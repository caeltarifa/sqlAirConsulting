-----SE ACTIVA CUANDO SE INSERTA NUEVO ELEMENTO AL PIB_TIEMPO_REAL
1 verifica la tabla pib_tiempo_real y elimina si fuera necesario
2 si fuera NOTAMR, actualiza es_pib=false de la tabla charly_repla
3 si fuera NOTAMC, actualiza es_pib=false de la tabla charly_cancel

''' --------------------------------------------------------------FUNCTION TRIGGER -----------------------------------------------------------------------------------------------'''
create or replace function control_automatico_pib() returns trigger as $control_automatico_pib$
declare
	reg RECORD;
	cur_notam CURSOR FOR SELECT distinct tn.id_notam_pib FROM
	(select id_notam_pib, split_part(id_notam_pib,' ',3)as codigo from plan_vuelo_pib_tiempo_real
	where (id_notam_pib like '%NOTAMR%') OR (id_notam_pib like '%NOTAMC%')
	)as t1
	inner join
	plan_vuelo_pib_tiempo_real as tn
	on
	tn.id_notam_pib like '(' || t1.codigo ||'%';
	
	BEGIN
	for reg in cur_notam 
	LOOP
		if reg is null then
		else
			delete from plan_vuelo_pib_tiempo_real where id_notam_pib like reg.id_notam_pib;
			update plan_vuelo_notam_trafico_charly_new set es_pib = 'false'
			where idnotam like reg.id_notam_pib || '%';
			update plan_vuelo_notam_trafico_charly_repla set es_pib = 'false' 
			where idnotam like '%' || reg.id_notam_pib;
		end if;
		--raise notice 'Procesando %', reg.id_notam_pib; 
	END LOOP;
	
	--ACTUALIZAMOS EL DOCUMENTO DE REGISTRO PIB EN LA BASE DE DATOS, REGISTRANDO LA ULTIMA MODIFICACION
	insert into aro_ais_historico_pib (lista_notam, fecha_modificado) values ( documentacion_notam_pib(), current_timestamp);
	return NEW;
end;
$control_automatico_pib$ LANGUAGE 'plpgsql';

''' --------------------------------------------------------------TRIGGER AFTER INSERT en pib_tiempo_real, verifica si es CANCEL o REPLACE y actualiza el PIB -----------------------------------------------------------------------------------------------'''
DROP TRIGGER IF EXISTS control_automatico_pib 
ON plan_vuelo_pib_tiempo_real  CASCADE  ;

CREATE TRIGGER control_automatico_pib AFTER INSERT 
    ON plan_vuelo_pib_tiempo_real 
	FOR EACH ROW EXECUTE PROCEDURE control_automatico_pib();	
	
--ACTUALIZAMOS EL DOCUMENTO DE REGISTRO PIB EN LA BASE DE DATOS, REGISTRANDO LA ULTIMA MODIFICACION
CREATE TRIGGER control_automatico_pib_before_delete BEFORE DELETE
    ON plan_vuelo_pib_tiempo_real 
	FOR EACH ROW EXECUTE PROCEDURE control_automatico_pib();	


'''-------------------------------EJEMPLO----'''
insert into plan_vuelo_pib_tiempo_real (id_notam_pib, hora_actualizacion) values ('(CPRU1/21 NOTAMR C0362/21',current_timestamp)
insert into plan_vuelo_pib_tiempo_real (id_notam_pib, hora_actualizacion) values ('(CPRU2/21 NOTAMR CPRU1/21',current_timestamp)
