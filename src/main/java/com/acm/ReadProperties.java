package com.acm;

import java.io.IOException;
import java.util.Properties;

public class ReadProperties {
	
	private static ReadProperties instance;
	
	private Properties props; 
	
	public static ReadProperties getInstance() {
		if(instance == null)
			instance = new ReadProperties();
		
		return instance;
	}
	
	
	private ReadProperties() {
		ClassLoader classLoader = getClass().getClassLoader();
		props = new Properties();
		try {
			props.load(classLoader.getResourceAsStream("/config.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	
	public String getProperty(String key) {
		return props.getProperty(key);
	}
	
}