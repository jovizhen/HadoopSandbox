package com.jovi.pig.util;

public enum WeatherRangeSpecIGRA2 {

	YEAR("14-17"), LAT("56-62"), LON("64-71"), TEMP("95-99");
	private String spec;
	
	WeatherRangeSpecIGRA2(String spec){
		this.spec = spec;
	}
	 public String spec(){
		 return spec;
	 }
}
