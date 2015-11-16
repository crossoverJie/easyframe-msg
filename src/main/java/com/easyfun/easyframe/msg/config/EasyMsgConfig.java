package com.easyfun.easyframe.msg.config;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Handle the configuration of easyframe-msg.properties
 * 
 * @author linzhaoming
 *
 * @Created 2014-07-01
 */
public class EasyMsgConfig {
	private static Log LOG = LogFactory.getLog(EasyMsgConfig.class);
	
	private static Properties prop = null;
	
	private static final String CONFIGURE_KEY = "msg.configure";
	
	private static final String CONFIGURE_FILE = "easyframe-msg.properties";
	
	static{
		try {
			String config = System.getProperty(CONFIGURE_KEY);
			if (StringUtils.isBlank(config)) {
				LOG.info("[EF-Msg] There does not exist JVM property '" + CONFIGURE_KEY + "' , so we will load from the default [easyframe-msg.properties] from classpath.");
				prop = loadPropertiesFromClassPath(CONFIGURE_FILE);
			} else {
				prop = loadPropertiesFromClassPath(config);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new RuntimeException("Init fail.", ex);
		}
	}
	
	private EasyMsgConfig() {
	}

	/** Get the basic Properties  */
	public static Properties getProperties() {
		return prop;
	}

	/** Set new Properties */
	public static void setProperties(Properties newProperties) {
		prop.clear();
		Set keys = newProperties.keySet();
		for (Iterator iter = keys.iterator(); iter.hasNext();) {
			Object item = (Object) iter.next();
			prop.put(item, newProperties.get(item));
		}
	}

	/** Get the special property from the configureation 
	 * @param prefix the key prefix
	 * @param isDiscardPrefix If set to ture, will discard the prefix when returned,
	 * */
	public static Properties getProperties(String prefix, boolean isDiscardPrefix) {
		Properties rtn = new Properties();
		String keydot = prefix + ".";
		Set key = prop.keySet();
		for (Iterator iter = key.iterator(); iter.hasNext();) {
			String element = (String) iter.next();
			int index = StringUtils.indexOf(element, prefix);
			if (index == 0) {
				if (isDiscardPrefix == true) {
					rtn.put(element.substring(keydot.length()).trim(), prop.get(element));
				} else {
					rtn.put(element, prop.get(element));
				}
			}
		}
		return rtn;
	}
	
	protected static String loadFileFromClassPath(String filePath) throws Exception {
		java.io.InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String tmp = null;
		StringBuffer sb = new StringBuffer();
		while (true) {
			tmp = br.readLine();
			if (tmp != null) {
				sb.append(tmp);
				sb.append("\n");
			} else {
				break;
			}
		}
		return sb.toString();
	}

	/** Get the URL from classpath or jar */
	protected static URL loadURLFromClassPath(String filePath) throws Exception {
		return Thread.currentThread().getContextClassLoader().getResource(filePath);
	}

	/** Get the InputStream from classpath or jar */
	private static InputStream loadInputStreamFromClassPath(String filePath) throws Exception {
		return Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
	}

	/** Get the PropertiesConfiguration from classpath or jar */
	protected static PropertiesConfiguration loadPropertiesConfigurationFromClassPath(String filePath) throws Exception {
		PropertiesConfiguration pc = new PropertiesConfiguration();
		pc.load(loadInputStreamFromClassPath(filePath));
		return pc;
	}

	/** Get the Properties from classpath or jar */
	private static Properties loadPropertiesFromClassPath(String filePath) throws Exception {
		Properties pc = new Properties();
		InputStream inputStream = loadInputStreamFromClassPath(filePath);
		if (inputStream == null) {
//			throw new Exception("InputStream is null, can not find file [" + filePath + "]");
			return pc;
		}
		pc.load(inputStream);
		return pc;
	}

	/** Get the Properties from classpath or jar
	 * @param prefix
	 * @param isDiscardPrefix if
	 *  */
	protected static Properties loadPropertiesFromClassPath(String filePath, String prefix, boolean isDiscardPrefix) throws Exception {
		Properties rtn = new Properties();
		Properties pc = loadPropertiesFromClassPath(filePath);
		Set key = pc.keySet();
		for (Iterator iter = key.iterator(); iter.hasNext();) {
			String element = (String) iter.next();
			int index = StringUtils.indexOf(element, prefix);
			if (index != -1) {
				if (isDiscardPrefix == true) {
					rtn.put(element.substring(index).trim(), pc.get(element));
				} else {
					rtn.put(element, pc.get(element));
				}
			}
		}
		return rtn;
	}

}
