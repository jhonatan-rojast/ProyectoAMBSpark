package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CargaDatosTXT {
	
	static final String ruta_fichero = "/root/eclipse-workspace_git/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) {
		final SparkConf sparkConf = new SparkConf().setAppName("CargaDatosTXT").setMaster("local");
		final JavaSparkContext spark = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		final String path = ruta_fichero.concat("/datosMeteorologicos.txt");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);

		datosMeteorologicos.printSchema();

		datosMeteorologicos.write().json(ruta_fichero.concat("/json"));

		spark.close();

	}

}
