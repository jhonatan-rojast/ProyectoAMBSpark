package com.ibm.proyectoamb.spark.bean;

public class Control_1 {
	
	private String fecha;
	private String hora;
	private String textoAviso;
	private String lugarAveria;
	private String nombreUsuario;
	
	public Control_1() {
		// TODO Auto-generated constructor stub
	}
		
	public Control_1(String fecha, String hora, String textoAviso, String lugarAveria, String nombreUsuario) {
		this.fecha = fecha;
		this.hora = hora;
		this.textoAviso = textoAviso;
		this.lugarAveria = lugarAveria;
		this.nombreUsuario = nombreUsuario;
	}

	public String getFecha() {
		return fecha;
	}
	public void setFecha(String fecha) {
		this.fecha = fecha;
	}
	public String getHora() {
		return hora;
	}
	public void setHora(String hora) {
		this.hora = hora;
	}
	public String getTextoAviso() {
		return textoAviso;
	}
	public void setTextoAviso(String textoAviso) {
		this.textoAviso = textoAviso;
	}
	public String getLugarAveria() {
		return lugarAveria;
	}
	public void setLugarAveria(String lugarAveria) {
		this.lugarAveria = lugarAveria;
	}
	public String getNombreUsuario() {
		return nombreUsuario;
	}
	public void setNombreUsuario(String nombreUsuario) {
		this.nombreUsuario = nombreUsuario;
	}
	
	
	

}
