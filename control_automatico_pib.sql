-----SE ACTIVA CUANDO SE INSERTA NUEVO ELEMENTO AL PIB_TIEMPO_REAL
1 verifica la tabla pib_tiempo_real y elimina si fuera necesario
2 si fuera NOTAMR, actualiza es_pib=false de la tabla charly_repla
3 si fuera NOTAMC, actualiza es_pib=false de la tabla charly_cancel

''' --------------------------------------------------------------FUNCTION TRIGGER -----------------------------------------------------------------------------------------------'''

''' --------------------------------------------------------------TRIGGER AFTER INSERT en pib_tiempo_real, verifica si es CANCEL o REPLACE y actualiza el PIB -----------------------------------------------------------------------------------------------'''
DROP TRIGGER IF EXISTS control_automatico_pib 
ON plan_vuelo_pib_tiempo_real  CASCADE  ;

CREATE TRIGGER control_automatico_pib AFTER INSERT 
    ON plan_vuelo_pib_tiempo_real 
	FOR EACH ROW EXECUTE PROCEDURE control_automatico_pib();	
'''-------------------------------EJEMPLO----'''
insert into plan_vuelo_pib_tiempo_real (id_notam_pib, hora_actualizacion) values ('(CPRU1/21 NOTAMR C0362/21',current_timestamp)
insert into plan_vuelo_pib_tiempo_real (id_notam_pib, hora_actualizacion) values ('(CPRU2/21 NOTAMR CPRU1/21',current_timestamp)