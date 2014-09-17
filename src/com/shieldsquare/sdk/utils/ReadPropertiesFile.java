package com.shieldsquare.sdk.utils;

import java.io.InputStream;
import java.util.Properties;


public class ReadPropertiesFile {
	public static String getProperty(String propertyName) {
		Properties prop = new Properties();
		String propertyValue = null;
		try {
			InputStream in = ReadPropertiesFile.class.getResourceAsStream("/file.properties");
			prop.load(in);
			propertyValue = prop.getProperty(propertyName);
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return propertyValue;
	}


}
