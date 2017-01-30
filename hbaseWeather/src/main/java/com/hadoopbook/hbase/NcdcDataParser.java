package com.hadoopbook.hbase;

import org.apache.commons.lang.StringUtils;

public class NcdcDataParser {
	private int airTemperature;
	private String year;
	
	private static final int VALUE_REMOVED = -8888;
	private static final int VALUE_MISSING = -9999;
	
	public boolean isValidRecord(String record) {
		if (!StringUtils.isEmpty(record) && record.charAt(0) != '#') {
			return true;
		}
		return false;
	}
	
	public boolean isValidTemperature() {
		return (airTemperature == VALUE_REMOVED 
				|| airTemperature == VALUE_MISSING) ? false : true;
	}
	
	public void parse(String header, String record){
		String airTempString = record.substring(22,27).trim();
		year = header.substring(13,17).trim();
		airTemperature = Integer.parseInt(airTempString);
	}

	public int getAirTemperature() {
		return airTemperature;
	}

	public void setAirTemperature(int airTemperature) {
		this.airTemperature = airTemperature;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}
}
