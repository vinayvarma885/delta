package com.gathi.mdm.delta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.esotericsoftware.minlog.Log;
import com.gathi.mdm.utils.configManagement.ConfigManager;

public class Preprocessor {
	
	final static Logger LOG = Logger.getLogger(Preprocessor.class);
	
	public static JSONObject process(JSONObject config, String selectedMode){
		
		// Step 1 : Input pre-processing
		config = processInputs(config, selectedMode);
		
		// Step 2 : Output pre-processing
		config = processOutputs(config, selectedMode);

		return config;
	} 
	
	private static JSONObject processInputs(JSONObject config, String selectedMode){
		// input check
		JSONObject inputs = null;
		try{
			inputs = config.getJSONObject("inputs");
		}catch(JSONException e){
			LOG.error("Mandatory Key 'inputs' not found in config file");
			System.exit(4);
		}
		if(!(inputs.has("current-source"))){
			LOG.error("Missing input : current-source");
			System.exit(4);
		}
		if(!(inputs.has("history-source"))){
			LOG.error("Missing input : history-source");
			System.exit(4);
		}
		if(selectedMode.equalsIgnoreCase("MD5") && !(inputs.has("history-md5-source"))){
			LOG.error("Missing inputs : history-md5-source");
			System.exit(4);
		}
		
		
		
		// validate input and add configs
		boolean allInputsAreValid = true;
		ConfigManager configManager = new ConfigManager();
		JSONObject sources = new JSONObject();
		Iterator<?> keys = inputs.keys();
		while(keys.hasNext()) {
			String key = (String) keys.next();
			JSONObject source = null;
			try {
			    source = inputs.getJSONObject(key);
			}catch(Exception e) {
				LOG.error("Delta expects exactly two inputs in FULL mode i.e. \n1) current data source and \n2) history data source\nAnd three"
						+ "inputs in MD5 mode \n1) current data source and \n2) history data source \n3) history md5 source");
				System.exit(4);
			}
			String sourceName = null;
			String userOrStar = "*";
			
			try{
				sourceName = source.getString("datasource");
			}catch(JSONException e){
				LOG.error(String.format("Mandatory Key 'datasource' not found for input %s. Correct the configFile", key));
				System.exit(4);
			}
			try{
				userOrStar = source.getString("user");
			}catch(JSONException e){
				LOG.error(String.format("Mandatory Key 'user' not found for input %s. Correct the configFile", key));
				System.exit(4);
			}
			if(configManager.isSourceValid(System.getProperty("ENV"), sourceName) == true){	
				String databaseOrStar = "*";	
				try{
					if(source.has("database")){
						databaseOrStar = source.getString("database");
					}
				}catch(Exception e){
					Log.error("**Preprocessor failed with ", e);
					System.exit(1);
				}
				try{
//					System.out.println("config " + configManager.getConfig(System.getProperty("ENV"), sourceName, databaseOrStar, userOrStar).toString());
					
					source.put("config", configManager.getConfig(System.getProperty("ENV"), sourceName, databaseOrStar, userOrStar));
					sources.put(key, source);
				}catch(JSONException e){
					LOG.error("Preprocessor failed with : ", e);
					System.exit(1);
				}
			}
			else{
				LOG.error("Unrecogonised Source Name : " + sourceName + " in 'inputs'");
				allInputsAreValid = false;
			}
		}
		if(allInputsAreValid==false){
			LOG.error("One or more of the sources in 'inputs' are unsupported");
			System.exit(4);
		}		
		try{
			config.put("inputs", sources);
		}catch(JSONException e){
			LOG.error("Preprocessor failed with : ", e);
			System.exit(1);
		}
		return config;
	}
	
	private static JSONObject processOutputs(JSONObject federationJSONConfig, String selectedMode){
		JSONArray outputs = null;
		try{
			outputs = federationJSONConfig.getJSONArray("outputs");
		} catch(JSONException e){
			LOG.error("Mandatory Key 'outputs' not found in configFile");
			System.exit(4);
		}
		if(outputs.length() < 1){
			LOG.error("Config file has no values for key 'outputs'. Atleast one output must be specified.");
			System.exit(4);
		}
		
		// validate output and add configs
		boolean allOutputsAreValid = true;
		ConfigManager configManager = new ConfigManager();
		for(int i = 0; i < outputs.length(); i++){
			JSONObject output = null;
			String outputName = null;
			String userOrStar = "*";
			try{
				output = outputs.getJSONObject(i); 
			} catch(Exception e){
				LOG.error(String.format("Failed to read source %s from 'outputs' in provided config file", i));	
				System.exit(4);
			}
			try{
				outputName = output.getString("datasink");
			}catch(JSONException e){
				LOG.error(String.format("Mandatory Key 'datasink' not found for output %s. Correct the configFile", i));
				System.exit(4);
			}
			try{
				userOrStar = output.getString("user");
			}catch(JSONException e){
				LOG.error(String.format("Mandatory Key 'user' not found for output %s. Correct the configFile", i));
				System.exit(4);
			}
			if(configManager.isSourceValid(System.getProperty("ENV"), outputName) == true){
				String DatabaseOrStar = "*";	
				try{
						if(output.has("database")){
							DatabaseOrStar = output.getString("database");
						}
				}catch(Exception e){
					LOG.error("**Preprocessor failed with : ", e);
					System.exit(1);
				}
				try{
//					System.out.println("config " + configManager.getConfig(System.getProperty("ENV"), outputName, DatabaseOrStar, userOrStar).toString());
					output.put("config", configManager.getConfig(System.getProperty("ENV"), outputName, DatabaseOrStar, userOrStar));
					outputs.put(i, output);
				}catch(JSONException e){
					LOG.error("Preprocessor failed with : ", e);
					System.exit(1);
				}
			}
			else{
				LOG.error("Found unsupported output type: " + outputName + " in 'outputs'");
				allOutputsAreValid = false;
			}
		}
		if(allOutputsAreValid==false){
			LOG.error("One or more of the output in 'outputs' are unsupported");
			System.exit(4);
		}		
		try{
			federationJSONConfig.put("outputs", outputs);
		}catch(JSONException e){
			LOG.error("Preprocessor failed with : ", e);
			System.exit(1);
		}
		return federationJSONConfig;
	}
}
