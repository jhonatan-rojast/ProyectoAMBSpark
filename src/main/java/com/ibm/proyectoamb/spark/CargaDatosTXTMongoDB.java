package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class CargaDatosTXTMongoDB {
	
	static final String ruta_fichero = "/root/eclipse-workspace_git/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) {
		
		final SparkConf sparkConf = new SparkConf()
									.setAppName("CargaDatosTXTConFiltroAPISparkSQL")
									.setMaster("local")
									.set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.temperatura")
									.set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.temperatura");
		
		final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();

		Dataset<Row> datosMeteorologicos = obtenerDatos(sqlContext);
	
		//datosMeteorologicos = datosMeteorologicos.select("Time", "Indoor_Temperature");
		//datosMeteorologicos = datosMeteorologicos.where("Indoor_Temperature < 10");
		
	    MongoSpark.save(datosMeteorologicos);
	    
	    Dataset<Row> df = MongoSpark.load(jsc).toDF();
        df.printSchema();
        df.show();
		
        jsc.close();
	}

	private static Dataset<Row> obtenerDatos(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/datosMeteorologicos.txt");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().format("com.crealytics.spark.excel").option("inferSchema", "true")
				.option("header", "true").option("delimiter", "\t").load(path);
		return datosMeteorologicos;
	}
}
