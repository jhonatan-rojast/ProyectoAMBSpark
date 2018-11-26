package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class CargaDatosTXTElasticSearch {
	
	static final String ruta_fichero = "/root/eclipse-workspace_git/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) {
		
		final SparkConf sparkConf = new SparkConf().setAppName("CargaDatosTXTElasticSearch").setMaster("local");
		sparkConf.set("es.nodes", "172.17.0.2");
		sparkConf.set("es.port", "9200");
		
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		//final SparkContext spark = new SparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = obtenerDatos(sqlContext);
		datosMeteorologicos = datosMeteorologicos.select("Time", "Indoor_Temperature");
		datosMeteorologicos = datosMeteorologicos.filter("Indoor_Temperature < 10");
				
		JavaEsSparkSQL.saveToEs(datosMeteorologicos, "proyectoambspark/datosmeteorologicos");

		spark.close();
	}
	
	private static Dataset<Row> obtenerDatos(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/datosMeteorologicos.txt");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}
}
