--Funcion para Encontrar del Cancelado su NOTAM NUEVO
create or replace function buscar_notam_cancel(varnotam varchar)returns varchar as $$
	declare cadena varchar;
	begin
		cadena := (select tn.idnotam from(select split_part(par.idnotam,' ',3) as varnotam from 
			plan_vuelo_notam_trafico_alfa_cancel par) as t1 
			inner join 
			plan_vuelo_notam_trafico_alfa_new as tn 
			on
			tn.idnotam like '(' || t1.varnotam);
		return cadena;
	end
$$ language plpgsql;

select buscar_notam_cancel('(A0132/21' || '%')

--Funcion para Encontrar del Cancelado su NOTAM REEMPLAZADO
create or replace function buscar_notam_cancel_repla(varnotam varchar)returns varchar as $$
	declare cadena varchar;
	begin
		cadena := (select tn.idnotam from(select split_part(par.idnotam,' ',3) as varnotam from 
			plan_vuelo_notam_trafico_alfa_cancel par) as t1 
			inner join 
			plan_vuelo_notam_trafico_alfa_repla as tr 
			on
			tr.idnotam like '(' || t1.varnotam || '%');
		return cadena;
	end
$$ language plpgsql;

select buscar_notam_cancel_repla('(A0132/21' || '%')

--Funcion para Encontrar del Reemplazo su NOTAM REEMPLAZADO
create or replace function buscar_notam_repla_repla(varnotam varchar)returns varchar as $$
	declare cadena varchar;
	begin
		cadena := (select tn.idnotam from(select split_part(par.idnotam,' ',3) as varnotam from 
			plan_vuelo_notam_trafico_alfa_repla par) as t1 
			inner join 
			plan_vuelo_notam_trafico_alfa_repla as tr 
			on
			tr.idnotam like '(' || t1.varnotam || '%');
		return cadena;
	end
$$ language plpgsql;

select buscar_notam_cancel_repla('(A0132/21' || '%')

--Funcion para Encontrar del Reemplazo su NOTAM NUEVO
create or replace function buscar_notam_repla_new(varnotam varchar)returns varchar as $$
	declare cadena varchar;
	begin
		cadena := (select tn.idnotam from(select split_part(par.idnotam,' ',3) as varnotam from 
			plan_vuelo_notam_trafico_alfa_repla par) as t1 
			inner join 
			plan_vuelo_notam_trafico_alfa_new as tr 
			on
			tr.idnotam like '(' || t1.varnotam || '%');
		return cadena;
	end
$$ language plpgsql;

select buscar_notam_cancel_repla('(A0132/21' || '%')