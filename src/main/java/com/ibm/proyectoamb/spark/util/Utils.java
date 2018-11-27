package com.ibm.proyectoamb.spark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Utils {

	
	public static Dataset<Row> alterarEstructuraCuerpoViaTunel(Dataset<Row> rows){
		
		rows = rows.withColumnRenamed("Ascendente", "ascendente");
		rows = rows.withColumnRenamed("Calzada", "calzada");
		rows = rows.withColumnRenamed("Cantidad", "cantidad");
		rows = rows.withColumnRenamed("Centre_de_control_del_funcionament_de_les_installacions", "centre_control_funcionament_les_installacions");
		rows = rows.withColumnRenamed("Codigo", "codigo");
		rows = rows.withColumnRenamed("Control_de_galib", "control_galib");
		rows = rows.withColumnRenamed("Data_Alta", "data_alta");
		rows = rows.withColumnRenamed("Data_Baixa", "data_baixa");
		rows = rows.withColumnRenamed("Data_Deteccio", "data_deteccio");
		rows = rows.withColumnRenamed("Enllumenat", "enllumenat");
		rows = rows.withColumnRenamed("Familia", "familia");
		rows = rows.withColumnRenamed("Galib_A", "galib_a");
		rows = rows.withColumnRenamed("Galib_B", "galib_b");
		rows = rows.withColumnRenamed("Galib__m", "galib_m");
		rows = rows.withColumnRenamed("Installacio_semaforica", "installacio_semaforica");
		rows = rows.withColumnRenamed("Lamina_impermeabilitzant_en_volta_B__m2", "lamina_impermeabilitzant_volta_b_m2");
		rows = rows.withColumnRenamed("Lamina_impermeabilitzant_en_volta__A__m2", "lamina_impermeabilitzant_volta_a_m2");
		rows = rows.withColumnRenamed("Longitud_A__m", "longitud_a_m");
		rows = rows.withColumnRenamed("Longitud_B__m", "longitud_b_m");
		rows = rows.withColumnRenamed("Longitud__m", "Longitud_m");
		rows = rows.withColumnRenamed("PK_final__km_m", "pk_final_km_m");
		rows = rows.withColumnRenamed("PK_inicial__km_m", "pk_inicial_km_m");
		rows = rows.withColumnRenamed("Pals_dauxili", "pals_dauxili");
		rows = rows.withColumnRenamed("Parets_laterals_A__m2", "parets_laterals_a_m2");
		rows = rows.withColumnRenamed("Parets_laterals_B__m2", "parets_laterals_b_m2");
		rows = rows.withColumnRenamed("Parets_laterals__m2", "parets_laterals_m2");
		rows = rows.withColumnRenamed("Resumen", "resumen");
		rows = rows.withColumnRenamed("Revestiment_funcional_en_parets_laterals_B__m2", "revestiment_funcional_parets_laterals_b_m2");
		rows = rows.withColumnRenamed("Revestiment_funcional_en_parets_laterals__A__m2", "revestiment_funcional_parets_laterals_a_m2");
		rows = rows.withColumnRenamed("Seguretat,_regulacio_i_control_de_transit", "seguretat_regulacio_control_transit");
		rows = rows.withColumnRenamed("Senyals_de_missatge_variable", "senyals_missatge_variable");
		rows = rows.withColumnRenamed("TV_en_circuit_tancat", "tv_circuit_tancat");
		rows = rows.withColumnRenamed("Tipus_Separacio", "tipus_separacio");
		rows = rows.withColumnRenamed("Unidad", "unidad");
		rows = rows.withColumnRenamed("Utmx", "utm_x");
		rows = rows.withColumnRenamed("Utmy", "utm_y");
		rows = rows.withColumnRenamed("Ventilacio", "ventilacio");
		rows = rows.withColumnRenamed("Volta_A__m2", "volta_a_m2");
		rows = rows.withColumnRenamed("Volta_B__m2", "volta_b_m2");
		rows = rows.withColumnRenamed("volta__m2", "volta_m2");
		
		return rows;
	}
}
