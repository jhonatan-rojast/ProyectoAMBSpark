package com.ibm.proyectoamb.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.ibm.proyectoamb.spark.bean.Control;
import com.mongodb.spark.MongoSpark;
public class CargaDatosJSONMongoDB {
	
	static final String ruta_fichero = "/root/eclipse-workspace_git/ProyectoAMBSpark/src/main/resources";

	public static void main(String[] args) throws InterruptedException {
		
		final SparkConf sparkConf = new SparkConf()
				.setAppName("CargaDatosJSONMongoDB")
				.setMaster("local")
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.scheduler.allocation.file", "/root/eclipse-workspace_git/ProyectoAMBSpark/src/main/resources/conf/conf-scheduler.xml")
				.set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/ambDB.control")
				.set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/ambDB.control");

		final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		final SparkSession sqlContext = SparkSession.builder().getOrCreate();
		
		/*** TODOS LOS CASOS EXCEPTO CSV ***/
		/* Dataset<Row> datosMeteorologicos = obtenerDatosJSON(sqlContext); 
		MongoSpark.save(datosMeteorologicos); 
		
		Dataset<Row> df = MongoSpark.load(jsc).toDF();
		df.printSchema();
		datosMeteorologicos.show();
		*/
		
		/*** CASO CSV ***/
		
		Dataset<Row> datosMeteorologicos = obtenerDatosCVS(sqlContext);
		MongoSpark.save(datosMeteorologicos); 
		
		Dataset<Row> df = MongoSpark.load(jsc).toDF();
		df.printSchema();
		jsc.close();
		
	}

	@SuppressWarnings("unused")
	private static Dataset<Row> obtenerDatosJSON(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/ingrid/carreteras.json");
		final Dataset<Row> datosMeteorologicos = sqlContext.read().json(path);
		return datosMeteorologicos;
	}
	
	@SuppressWarnings("unused")
	private static Dataset<Row> obtenerDatosXLS(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/ingrid/carreteras.xls");
		final Dataset<Row> datosMeteorologicos = sqlContext.read()
												.option("useHeader", "true")
										        .format("com.crealytics.spark.excel")
												.load(path);
		return datosMeteorologicos;
	}
	
	@SuppressWarnings("unused")
	private static Dataset<Row> obtenerDatosCVS(final SparkSession sqlContext) {
		final String path = ruta_fichero.concat("/scada/Control1.csv");
		
		JavaRDD<Control> bean = sqlContext
								.read()
								.textFile(path)
											.javaRDD().map(new Function<String, Control>() {
												public Control call(String line) throws Exception {
													 
													  String[] arrays = line. split(";");
													  
											          Control c = new Control();
											          c.setFecha(validarCaracteresEspeciales(arrays[0]));
											          c.setHora(validarCaracteresEspeciales(arrays[1]));
											          c.setTextoAviso(validarCaracteresEspeciales(arrays[2]));
											          c.setLugarAveria(validarCaracteresEspeciales(arrays[3]));
											          c.setNombreUsuario(validarCaracteresEspeciales(arrays[4]));
											          
											          return c;
											        }
											});
		
		 Dataset<Row> rows = sqlContext.createDataFrame(bean, Control.class);
		 
		 final Row header= rows.first();
		 
		 Dataset<Row> dataPointsWithoutHeader = rows.filter(new FilterFunction<Row>() {
			public boolean call(Row row) throws Exception {
				    return !row.equals(header);
			    }
		});
		
		 dataPointsWithoutHeader.show();
		 
		return dataPointsWithoutHeader;
	}
	
	public static String validarCaracteresEspeciales(String input) {
		  // Cadena de caracteres original a sustituir.
		  String original = "áàäéèëíìïóòöúùuñÁÀÄÉÈËÍÌÏÓÒÖÚÙÜÑçÇ";
		  // Cadena de caracteres ASCII que reemplazarán los originales.
		  String ascii = "aaaeeeiiiooouuunAAAEEEIIIOOOUUUNcC";
		  String output = input;
		  for (int i=0; i<original.length(); i++) {
		      // Reemplazamos los caracteres especiales.
		      output = output.replace(original.charAt(i), ascii.charAt(i));
		  }//for i
		  return output.replaceAll("\\\"", "");
		}
	
}
