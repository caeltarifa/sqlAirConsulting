''' EL TRIGGER SE ACTIVA CUANDO SE INSERTA EN LA TABLA CHARLY_NEW CHARLY_REPLA CHARLY_CANCEL y lo agrega en la tabla PIB_TEMPO_REAL'''
''' hacer pib automatico, despues para cuando se inserte un charly cancel en plan_vuelo_notam_trafico_charly_cancel insertar en pib_tiempo_real -----------------------------------------------------------------------------------------------'''
''' hacer pib automatico, despues para cuando se inserte un charly replac en plan_vuelo_notam_trafico_charly_repla insertar en pib_tiempo_real -----------------------------------------------------------------------------------------------'''

CREATE OR REPLACE FUNCTION update_pib_after_insert_cancel_repla() RETURNS TRIGGER AS $update_pib_after_insert_cancel_repla$
  DECLARE
  BEGIN
 	  insert into plan_vuelo_pib_tiempo_real (id_notam_pib, hora_actualizacion) values (NEW.idnotam,current_timestamp);
  return NEW;
  END;
$update_pib_after_insert_cancel_repla$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_pib_after_insert_cancel_repla 
ON plan_vuelo_notam_trafico_charly_cancel, plan_vuelo_notam_trafico_charly_repla CASCADE  ;

CREATE TRIGGER update_pib_after_insert_cancel_repla AFTER INSERT 
    ON plan_vuelo_notam_trafico_charly_new 
	FOR EACH ROW EXECUTE PROCEDURE update_pib_after_insert_cancel_repla();	

CREATE TRIGGER update_pib_after_insert_cancel_repla AFTER INSERT 
    ON plan_vuelo_notam_trafico_charly_cancel 
	FOR EACH ROW EXECUTE PROCEDURE update_pib_after_insert_cancel_repla();	

CREATE TRIGGER update_pib_after_insert_cancel_repla AFTER INSERT 
    ON plan_vuelo_notam_trafico_charly_repla 
	FOR EACH ROW EXECUTE PROCEDURE update_pib_after_insert_cancel_repla();	

'''---- ejemplo ----'''
insert into plan_vuelo_notam_trafico_charly_new (id_mensaje_c_n,aftn1,aftn2,idnotam,resumen,aplica_a,valido_desde,valido_hasta,mensaje,es_pib,ingresado) values ('ZPX0275PRUEBA/12032021','GG SLZZYNYX','121915 SLLPYNYX','(CPRUE/21 NOTAMN','Q) SLLF/QFALC/V/NBO/A/000/999/1315S06436W005','A) SLRA
','B) 2103121915
','C) 2103122300
','E) AD CLSD MAINT RWY)
','t',current_timestamp);

select * from plan_vuelo_pib_tiempo_real where id_notam_pib like '%PR%'
