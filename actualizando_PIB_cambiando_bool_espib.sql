CREATE OR REPLACE FUNCTION actualizar_pib_despuesde_boolean_changes()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	IF NEW.es_pib THEN
		 INSERT INTO plan_vuelo_pib_tiempo_real(id_notam_pib,hora_actualizacion)
		 VALUES(NEW.idnotam,current_timestamp);
	ELSE
		delete from plan_vuelo_pib_tiempo_real where id_notam_pib like NEW.idnotam;
	END IF;

	RETURN NEW;
END;
$$

CREATE TRIGGER despues_de_cambiar_boolean_pib
  BEFORE UPDATE
  ON plan_vuelo_notam_trafico_charly_new
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_pib_despuesde_boolean_changes();
  
CREATE TRIGGER despues_de_cambiar_boolean_pib
  BEFORE UPDATE
  ON plan_vuelo_notam_trafico_charly_repla
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_pib_despuesde_boolean_changes();
  
CREATE TRIGGER despues_de_cambiar_boolean_pib
  BEFORE UPDATE
  ON aro_ais_notam_trafico_charly_new
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_pib_despuesde_boolean_changes();
  
CREATE TRIGGER despues_de_cambiar_boolean_pib
  BEFORE UPDATE
  ON aro_ais_notam_trafico_charly_repla
  FOR EACH ROW
  EXECUTE PROCEDURE actualizar_pib_despuesde_boolean_changes();
