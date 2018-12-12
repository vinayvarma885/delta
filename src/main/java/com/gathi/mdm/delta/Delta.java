package com.gathi.mdm.delta;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.jute.compiler.generated.ParseException;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.functions;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.apache.spark.sql.functions;
import scala.collection.JavaConverters;
import scala.collection.JavaConversions;
import com.esotericsoftware.minlog.Log;

import scala.collection.Seq;
import scala.collection.mutable.Buffer;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;

public class Delta {

	final static Logger LOG = Logger.getLogger(Delta.class);
	final static String EFFECTIVE_END_TIMESTAMP = "EFF_END_TS";
	final static String FUTURE_DATE = "9999-12-31 00:00:00";
	
	public static void main(String[] args) {
		String appName = null;
		String configFilePath = null;
		String env = null;
		String LOG_PATH = null;
		String history_mode = null;
		String SPARK_SQL_WAREHOUSE_DIR = null;
	//	String AUDIT_COLUMNS = null;
		
		Options options = new Options();
		options.addOption("APP_NAME", true, "Application Name");
		options.addOption("CONFIG_FILE_PATH", true, "Config file location");
		options.addOption("LOG_PATH", true, "Log file path");
		options.addOption("ENV", true, "Enviornment : dev/uat/prod");
		options.addOption("HISTORY_MODE", true, "History file : FULL/MD5.\n FULL is actual history full file.\n MD5 is a file having only 3 columns i.e pk, md5(pk), md5(cdc)");
		options.addOption("SPARK_SQL_WAREHOUSE_DIR", true, "spark.sqp.warehouse.dir value from hive-site.xml");
		options.addOption("AUDIT_COLUMNS", true, "csv of AUDIT_COLUMNS used in hive tables");
		
		CommandLineParser parser = new PosixParser();
		try {
			CommandLine line = parser.parse(options, args);
			if(line!=null) {
				if(line.hasOption("APP_NAME")){
					appName = line.getOptionValue("APP_NAME");
				}else{
					throw new ParseException("Arg: APP_NAME is missing");
				}
				if(line.hasOption("CONFIG_FILE_PATH")){
					configFilePath = line.getOptionValue("CONFIG_FILE_PATH");
				}else{
					throw new ParseException("Arg: CONFIG_FILE_PATH is missing");
				}
				if(line.hasOption("ENV")){
					env = line.getOptionValue("ENV");
				}else{
					throw new ParseException("Arg: ENV is missing");
				}
				if(line.hasOption("LOG_PATH")){
					LOG_PATH = line.getOptionValue("LOG_PATH");
				}else{
					throw new ParseException("Arg: LOG_PATH is missing");
				}
				if(line.hasOption("HISTORY_MODE")){
					history_mode = line.getOptionValue("HISTORY_MODE");
				}else{
					throw new ParseException("Arg: HISTORY_MODE is missing");
				}
				if(history_mode.equalsIgnoreCase("hive")) {
					SPARK_SQL_WAREHOUSE_DIR = line.getOptionValue("SPARK_SQL_WAREHOUSE_DIR");
				//	AUDIT_COLUMNS = line.getOptionValue("AUDIT_COLUMNS");
				}
			}else {
				throw new ParseException("Missing Arguments, See Usage.");
			}
		}catch(Exception e) {
			e.printStackTrace(System.out);
			System.out.println("spark-submit");
		}
		
		System.setProperty("ENV", env);
		System.setProperty("LOG_PATH", LOG_PATH);
	//	DeltaLogger.configureLogger(LOG_PATH);
		SparkSession spark = null;
		if(history_mode.equalsIgnoreCase("hive")) {
			String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
			spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
			
		}
		else
			spark = SparkSession.builder().appName(appName).getOrCreate();
		
		spark.conf().set("spark.sql.shuffle.partitions", 500);
		JSONObject deltaJSONConfig = null;
		try{
			deltaJSONConfig = new JSONObject(com.gathi.io.IO.getFileContent(configFilePath));
		}catch(IOException e){
			LOG.error("Failed to read config file", e);
			System.exit(2);
		}catch(JSONException e){
			LOG.error("Incorrectly Specified JSON config", e);
			System.exit(4);
		}
		
		try{
			if(history_mode.equalsIgnoreCase("FULL"))
				delta(spark, deltaJSONConfig);
			else if(history_mode.equalsIgnoreCase("MD5"))
				deltaWithMD5Hist(spark, deltaJSONConfig);
			else if(history_mode.equalsIgnoreCase("HIVE")) {
				deltaWithHistoryFromHiveView(spark, deltaJSONConfig);
			}
			else
				throw new Exception("Invalid History mode specified. Valid Values are: FULL/MD5");
		}catch(Exception e){
			LOG.error("Delta failed with ", e);
			spark.stop();
			System.exit(1);
		}
		LOG.info("Delta successfuly completed");
		spark.stop();
		System.exit(0);
	}
	
	private static boolean delta(SparkSession spark, 
			JSONObject deltaJSONConfig) throws Exception{

		// step 1: pre-processing : validation, 
		deltaJSONConfig = Preprocessor.process(deltaJSONConfig, "FULL");
		
		// step 2: fetch history open records as DF & history pks
		JSONObject historyConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("history-source");
		Map<String, List<String>> historyFieldsDict = getFieldsDict(historyConf.getJSONArray("schema"));		
		Dataset<Row> history = com.gathi.io.IO.readAsDataFrames(historyConf, spark);//.limit(5); //.filter(String.format("%s = '%s'",EFFECTIVE_END_TIMESTAMP, FUTURE_DATE));
		history = history.persist();
		long histCount = history.count();
	//	long historyCount = history.count();
		
	//	history.show();
		// step 3: fetch current as DF & current pks; reorder 
		JSONObject currentConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("current-source");
	//	Map<String, List<String>> currentFieldsDict = getFieldsDict(currentConf.getJSONArray("schema"));			
		Dataset<Row> current = com.gathi.io.IO.readAsDataFrames(currentConf, spark);//.limit(5);
		current.persist();
		long currentCount = current.count();
	//	current.show();
		if(0!=0) {
			//all inserts
			System.out.println("\n\n\n\t\t\t\t DAY ONE INSERTS \n\n\n\n\n");
			current = current.withColumn("IUDM_INDICATOR", functions.lit("I"));
			JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
			com.gathi.io.IO.writeDFtoTarget(outputJSONs, current, spark);
			System.exit(0);
			
		}
		
		// step 6: history and current columns
		String historyFields = String.join(", ", history.columns());
		String currentFields = String.join(", ", current.columns());
		
		// step 4: generate DF with md5 of pk & cdc for both history and current
		history = generateDFWithMD5Fields(history, historyFieldsDict, spark);
		current = generateDFWithMD5Fields(current, historyFieldsDict, spark);
		
		// step 4: rename DF fields with history and current prefix and register as temp views
	//	history = renameFieldsWithPrefix(history, "history_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
	//	current = renameFieldsWithPrefix(current, "current_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		history.show();
		current.show();
		System.out.println("\n=========================================== 1\n");
		
		
		history.createTempView("history");
		current.createTempView("current");
	 //   history.toDF(historyFieldsDict.get(""));
		// step 5: full outer join & register as temp view
	//	String joinStmt = "SELECT history_pk_md5, current_pk_md5, history_cdc_md5, current_cdc_md5 FROM history FULL OUTER JOIN current ON history.history_pk_md5 = current.current_pk_md5";
		String joinStmt = "SELECT history.pk_md5 AS history_pk_md5, current.pk_md5 AS current_pk_md5,"
				+ "history.cdc_md5 AS  history_cdc_md5, current.cdc_md5 AS current_cdc_md5 FROM history FULL OUTER JOIN current ON history.pk_md5 = current.pk_md5";
		
		Dataset<Row> joinedDF = spark.sql(joinStmt);
		joinedDF.persist(StorageLevel.MEMORY_ONLY());
		joinedDF.createTempView("joinedDF");
		//spark.sql("cache table joinedDF");
		
		joinedDF.show();
		System.out.println("\n=========================================== 2\n");
		//System.exit(99);

		
		
		
		String selectInserts = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_pk_md5 IS NULL";
		String selectDeletes = "SELECT history_pk_md5 AS pk_md5 FROM joinedDF where current_pk_md5 IS NULL";
		String selectUpdates = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_cdc_md5 <> current_cdc_md5";
	//	String selectMatches = "SELECT " + currentFields + ", 'M' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 = current_cdc_md5 ";
		
		Dataset<Row> inserts = spark.sql(selectInserts);
		Dataset<Row> deletes = spark.sql(selectDeletes);
		Dataset<Row> updates = spark.sql(selectUpdates);
	//	Dataset<Row> matches = spark.sql(selectMatches);
		
	//	inserts = removePrefix("current_", inserts, spark);
	//	deletes = removePrefix("history_", deletes, spark);
	//	updates = removePrefix("current_", updates, spark);
		
		inserts.createOrReplaceTempView("inserts");
		deletes.createOrReplaceTempView("deletes");
		updates.createOrReplaceTempView("updates");
		
		long insertFOJ = inserts.count();
		long deletesFOJ = deletes.count();
		long updatesFOJ = updates.count();
	//	inserts.show();
	//	deletes.show();
	//	updates.show();
		
		System.out.println("=================== ==================== ================ =================== =============== =============");
		System.out.println("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		System.out.println("=================== ==================== ================ =================== =============== =============");
		inserts = spark.sql("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		deletes = spark.sql("Select " + historyFields + ",'D' AS IUDM_INDICATOR FROM deletes left join history on deletes.pk_md5 = history.pk_md5");
		updates = spark.sql("Select " + currentFields + ",'U' AS IUDM_INDICATOR FROM updates left join current on updates.pk_md5 = current.pk_md5");
		
		
	/*	inserts.show();
		deletes.show();
		updates.show();*/
		// step 7: remove Prefix
		/*inserts = removePrefix("current_", inserts, spark);
		deletes = removePrefix("history_", deletes, spark);
		updates = removePrefix("current_", updates, spark);*/
		long insertCount = inserts.count();
		long deletsCount = deletes.count();
		long updateCound = updates.count();
		
		//step 8: union all
		Dataset<Row> output = inserts.union(deletes).union(updates);//.union(matches);
		
		//step 9: drop md5_pk md5_cdc fields
	//	output = output.drop("pk_md5");
	//	output = output.drop("cdc_md5");
		output = output.repartition(1);
		output.persist();
		long outputCount = output.count();
		System.out.println("\n\n\n\n =========================>>====================>>====================>>====================>>>>> "  );
		System.out.println("History count: " + histCount);
		System.out.println("Current count: " + currentCount);
		System.out.println("insertFOJ count: " + insertFOJ);
		System.out.println("deletesFOJ count: " + deletesFOJ);
		System.out.println("updatesFOJ count: " + updatesFOJ);
		System.out.println("insertCount count: " + insertCount);
		System.out.println("deletsCount count: " + deletsCount);
		System.out.println("updateCound count: " + updateCound);
	//	output.show();
		// step 10:  write data-frame to outputs
		JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
		com.gathi.io.IO.writeDFtoTarget(outputJSONs, output, spark);
		
		return true;
	}
	
	private static boolean deltaWithHistoryFromHiveView(SparkSession spark, 
			JSONObject deltaJSONConfig) throws Exception{

		// step 1: pre-processing : validation, 
		deltaJSONConfig = Preprocessor.process(deltaJSONConfig, "HIVE");
		
		// step 2: fetch history open records as DF & history pks
		JSONObject historyConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("history-source");
		Map<String, List<String>> historyFieldsDict = getFieldsDict(historyConf.getJSONArray("schema"));		
		
		JSONObject currentConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("current-source");
		Map<String, List<String>> currentFieldsDict = getFieldsDict(currentConf.getJSONArray("schema"));	
		
	    String dbTable = String.format("%s.%s", historyConf.getString("database"), historyConf.getString("table"));
		
		String activeRecordSelectStmt = String.format("select %s from %s where %s = '%s'",
				String.join(", ",currentFieldsDict.get("all_fields")),
				dbTable,
				EFFECTIVE_END_TIMESTAMP,
				FUTURE_DATE);
		System.out.println("activeRecordSelectStmt: " + activeRecordSelectStmt);
	//	spark.catalog().refreshTable(String.format("%s.%s",historyConf.getString("database"), historyConf.getString("table")));
		Dataset<Row> current = com.gathi.io.IO.readAsDataFrames(currentConf, spark);
		current.persist();
		System.out.println("Current Day Count: " + current.count());
		System.out.println(">> " + spark.catalog().databaseExists(historyConf.getString("database")));
		System.out.println(">> " + spark.catalog().tableExists(historyConf.getString("table")));
		if(!spark.catalog().databaseExists(historyConf.getString("database")) || !spark.catalog().tableExists(historyConf.getString("table"))) {
			System.out.println("\n\n\n\t\t\t\t DAY ONE INSERTS \n\n\n\n\n");
			current = current.withColumn("ACTION_CODE", functions.lit("I"));
			JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
			com.gathi.io.IO.writeDFtoTarget(outputJSONs, current, spark);
			System.exit(0);
		}
		
		Dataset<Row> history =spark.sql(activeRecordSelectStmt);
		history = history.persist();
		long histCount = history.count();
		System.out.println("History Count " + histCount);

		if(histCount==0) {
			//all inserts
			System.out.println("\n\n\n\t\t\t\t DAY ONE INSERTS \n\n\n\n\n");
			current = current.withColumn("ACTION_CODE", functions.lit("I"));
			JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
			com.gathi.io.IO.writeDFtoTarget(outputJSONs, current, spark);
			System.exit(0);
			
		}
		// step 3: fetch current as DF & current pks; reorder 
		//.limit(5);
		
	//	current.show();
	
		
		long currentCount = current.count();
		// step 6: history and current columns
		String historyFields = String.join(", ", history.columns());
		String currentFields = String.join(", ", current.columns());
		
		// step 4: generate DF with md5 of pk & cdc for both history and current
		history = generateDFWithMD5Fields(history, historyFieldsDict, spark);
		current = generateDFWithMD5Fields(current, historyFieldsDict, spark);
		
		// step 4: rename DF fields with history and current prefix and register as temp views
	//	history = renameFieldsWithPrefix(history, "history_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
	//	current = renameFieldsWithPrefix(current, "current_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		history.show();
		current.show();
		System.out.println("\n=========================================== 1\n");
		
		
		history.createTempView("history");
		current.createTempView("current");
	 //   history.toDF(historyFieldsDict.get(""));
		// step 5: full outer join & register as temp view
	//	String joinStmt = "SELECT history_pk_md5, current_pk_md5, history_cdc_md5, current_cdc_md5 FROM history FULL OUTER JOIN current ON history.history_pk_md5 = current.current_pk_md5";
		String joinStmt = "SELECT history.pk_md5 AS history_pk_md5, current.pk_md5 AS current_pk_md5,"
				+ "history.cdc_md5 AS  history_cdc_md5, current.cdc_md5 AS current_cdc_md5 FROM history FULL OUTER JOIN current ON history.pk_md5 = current.pk_md5";
		
		Dataset<Row> joinedDF = spark.sql(joinStmt);
		joinedDF.persist(StorageLevel.MEMORY_ONLY());
		joinedDF.createTempView("joinedDF");
		//spark.sql("cache table joinedDF");
		
		joinedDF.show();
		System.out.println("\n=========================================== 2\n");
		//System.exit(99);

		
		
		
		String selectInserts = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_pk_md5 IS NULL";
		String selectDeletes = "SELECT history_pk_md5 AS pk_md5 FROM joinedDF where current_pk_md5 IS NULL";
		String selectUpdates = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_cdc_md5 <> current_cdc_md5";
	//	String selectMatches = "SELECT " + currentFields + ", 'M' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 = current_cdc_md5 ";
		
		Dataset<Row> inserts = spark.sql(selectInserts);
		Dataset<Row> deletes = spark.sql(selectDeletes);
		Dataset<Row> updates = spark.sql(selectUpdates);
	//	Dataset<Row> matches = spark.sql(selectMatches);
		
	//	inserts = removePrefix("current_", inserts, spark);
	//	deletes = removePrefix("history_", deletes, spark);
	//	updates = removePrefix("current_", updates, spark);
		
		inserts.createOrReplaceTempView("inserts");
		deletes.createOrReplaceTempView("deletes");
		updates.createOrReplaceTempView("updates");
		
		long insertFOJ = inserts.count();
		long deletesFOJ = deletes.count();
		long updatesFOJ = updates.count();
	//	inserts.show();
	//	deletes.show();
	//	updates.show();
		
		System.out.println("=================== ==================== ================ =================== =============== =============");
		System.out.println("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		System.out.println("=================== ==================== ================ =================== =============== =============");
		inserts = spark.sql("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		deletes = spark.sql("Select " + historyFields + ",'D' AS IUDM_INDICATOR FROM deletes left join history on deletes.pk_md5 = history.pk_md5");
		updates = spark.sql("Select " + currentFields + ",'U' AS IUDM_INDICATOR FROM updates left join current on updates.pk_md5 = current.pk_md5");
		
		
	/*	inserts.show();
		deletes.show();
		updates.show();*/
		// step 7: remove Prefix
		/*inserts = removePrefix("current_", inserts, spark);
		deletes = removePrefix("history_", deletes, spark);
		updates = removePrefix("current_", updates, spark);*/
		long insertCount = inserts.count();
		long deletsCount = deletes.count();
		long updateCound = updates.count();
		
		//step 8: union all
		Dataset<Row> output = inserts.union(deletes).union(updates);//.union(matches);
		
		//step 9: drop md5_pk md5_cdc fields
	//	output = output.drop("pk_md5");
	//	output = output.drop("cdc_md5");
		output = output.repartition(1);
		output.persist();
		long outputCount = output.count();
		System.out.println("\n\n\n\n =========================>>====================>>====================>>====================>>>>> "  );
		System.out.println("History count: " + histCount);
		System.out.println("Current count: " + currentCount);
		System.out.println("insertFOJ count: " + insertFOJ);
		System.out.println("deletesFOJ count: " + deletesFOJ);
		System.out.println("updatesFOJ count: " + updatesFOJ);
		System.out.println("insertCount count: " + insertCount);
		System.out.println("deletsCount count: " + deletsCount);
		System.out.println("updateCound count: " + updateCound);
	//	output.show();
		// step 10:  write data-frame to outputs
		JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
		com.gathi.io.IO.writeDFtoTarget(outputJSONs, output, spark);
		
		return true;
	}
	private static boolean deltaWithMD5Hist(SparkSession spark, 
			JSONObject deltaJSONConfig) throws Exception{

		// step 1: pre-processing : validation, 
		deltaJSONConfig = Preprocessor.process(deltaJSONConfig, "MD5");
		
		// step 2: fetch history open records as DF & history pks
		JSONObject historyConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("history-source");
		Map<String, List<String>> historyFieldsDict = getFieldsDict(historyConf.getJSONArray("schema"));	
		
		//checks
		if(historyFieldsDict.get("pk_fields").size() == 0) {
			System.out.println("No Primary keys specified for history.");
			System.exit(4);
		}
		if(historyFieldsDict.get("cdc_fields").size() == 0) {
			System.out.println("No CDC keys specified for history.");
			System.exit(4);
		}
		Dataset<Row> history = com.gathi.io.IO.readAsDataFrames(historyConf, spark);
		
		//step 3: fetch history md5
		JSONObject historyMD5Conf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("history-md5-source");
		Map<String, List<String>> historyMD5FieldsDict = getFieldsDict(historyMD5Conf.getJSONArray("schema"));	
		Dataset<Row> historyMD5 = com.gathi.io.IO.readAsDataFrames(historyMD5Conf, spark);
		//.limit(10); //.filter(String.format("%s = '%s'",EFFECTIVE_END_TIMESTAMP, FUTURE_DATE));
	//	history.show();
		// step 3: fetch current as DF & current pks; reorder 
		JSONObject currentConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("current-source");
		Map<String, List<String>> currentFieldsDict = getFieldsDict(currentConf.getJSONArray("schema"));			
		Dataset<Row> current = com.gathi.io.IO.readAsDataFrames(currentConf, spark);//.limit(10);//.limit(500);
		
		// step 4: generate DF with md5 of pk & cdc for current
	//	history = generateDFWithMD5Fields(history, historyFieldsDict, spark);
		current = generateDFWithMD5Fields(current, currentFieldsDict, spark);
		
		// step 4: rename DF fields with history and current prefix and register as temp views
		historyMD5 = renameFieldsWithPrefix(historyMD5, "history_", spark);
	//	current = renameFieldsWithPrefix(current, "current_", spark);
		
		historyMD5.show();
	    current.show();
		
	    historyMD5.createTempView("historyMD5");
		current.createTempView("current");
	 //   history.toDF(historyFieldsDict.get(""));
		// step 5: full outer join & register as temp view
		String joinStmt = "SELECT * FROM history FULL OUTER JOIN current ON historyMD5.history_pk_md5 = current.pk_md5";
		Dataset<Row> joinedDF = spark.sql(joinStmt);
		joinedDF.createTempView("joinedDF");
	//	joinedDF.persist();
	//	joinedDF.show();
		// step 6: history and current columns
	//	String historyFields = String.join(", ", history.columns());
	//	String currentFields = String.join(", ", current.columns());
		
	/*	String selectInserts = "SELECT concat_ws('_', " + String.join(",", currentFieldsDict.get("pk_fields")) + ") AS pk, pk_md5, cdc_md5, 'I' as IUDM_INDICATOR FROM joinedDF where history_pk_md5 IS NULL";
		String selectDeletes = "SELECT history_pks as pk, history_pk_md5 AS pk_md5, history_cdc_md5 AS cdc_md5, 'D' as IUDM_INDICATOR FROM joinedDF where pk_md5 IS NULL";
		String selectUpdates = "SELECT concat_ws('_', " + String.join(",", currentFieldsDict.get("pk_fields")) + ") AS pk, pk_md5, cdc_md5, 'U' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 <> cdc_md5";
	
		*/
		
		
		String selectInserts = "SELECT " + String.join(",", currentFieldsDict.get("all_fields")) + ", 'I' as IUDM_INDICATOR FROM joinedDF where history_pk_md5 IS NULL";
	//	String selectDeletes = "SELECT history_pks as pk, 'D' as IUDM_INDICATOR FROM joinedDF where pk_md5 IS NULL";
		String selectUpdates = "SELECT " + String.join(",", currentFieldsDict.get("all_fields")) + ", 'U' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 <> cdc_md5";
	//	String selectMatches = "SELECT " + currentFields + ", 'M' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 = current_cdc_md5 ";
		
		
		String selectDeletes = "SELECT history_pks as pk, 'D' as IUDM_INDICATOR FROM joinedDF where pk_md5 IS NULL";
		
		Dataset<Row> inserts = spark.sql(selectInserts);
		inserts.show();
	//	Dataset<Row> deletes = spark.sql(selectDeletes);
		Dataset<Row> updates = spark.sql(selectUpdates);
	//	Dataset<Row> matches = spark.sql(selectMatches);

		
		// step 7: remove Prefix
		inserts = removePrefix("current_", inserts, spark);
	//	deletes = removePrefix("history_", deletes, spark);
		updates = removePrefix("current_", updates, spark);
		
		//step 8: union all
		//Dataset<Row> output = inserts.union(deletes).union(updates);//.union(matches);
		Dataset<Row> output = inserts.union(updates);//.union(matches);
		//step 9: drop md5_pk md5_cdc fields
	//	output = output.drop("pk_md5");
	//	output = output.drop("cdc_md5");
		output = output.repartition(1);
	//	output.show();
		// step 10:  write data-frame to outputs
		JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
		com.gathi.io.IO.writeDFtoTarget(outputJSONs, output, spark);
		
		return true;
	}
	
	private static boolean deltaNew(SparkSession spark, 
			JSONObject deltaJSONConfig) throws Exception{

		// step 1: pre-processing : validation, 
		deltaJSONConfig = Preprocessor.process(deltaJSONConfig, "HIVE");
		
		// step 2: fetch history open records as DF & history pks
		JSONObject historyConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("history-source");
		Map<String, List<Column>> historyFieldsDict = getFieldsDictNew(historyConf.getJSONArray("schema"));		
		
		JSONObject currentConf = deltaJSONConfig.getJSONObject("inputs").getJSONObject("current-source");
		Map<String, List<Column>> currentFieldsDict = getFieldsDictNew(currentConf.getJSONArray("schema"));	
		
	    String dbTable = String.format("%s.%s", historyConf.getString("database"), historyConf.getString("table"));
		
		
	//	spark.catalog().refreshTable(String.format("%s.%s",historyConf.getString("database"), historyConf.getString("table")));
		Dataset<Row> current = com.gathi.io.IO.readAsDataFrames(currentConf, spark);
		current.persist();
		System.out.println(">> " + spark.catalog().databaseExists(historyConf.getString("database")));
		System.out.println(">> " + spark.catalog().tableExists(historyConf.getString("table")));
		/*if(!spark.catalog().databaseExists(historyConf.getString("database")) || !spark.catalog().tableExists(historyConf.getString("table"))) {
			System.out.println("\n\n\n\t\t\t\t DAY ONE INSERTS \n\n\n\n\n");
			current = current.withColumn("ACTION_CODE", functions.lit("I"));
			JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
			com.gathi.io.IO.writeDFtoTarget(outputJSONs, current, spark);
			System.exit(0);
		}*/
		String activeRecordSelectStmt = String.format("select * from %s where %s = '%s'",
				dbTable,
				EFFECTIVE_END_TIMESTAMP,
				FUTURE_DATE);
		Dataset<Row> history = spark.sql(activeRecordSelectStmt);
		history = history.persist();
		long histCount = history.count();
		

		/*if(histCount==0) {
			//all inserts
			System.out.println("\n\n\n\t\t\t\t DAY ONE INSERTS \n\n\n\n\n");
			current = current.withColumn("ACTION_CODE", functions.lit("I"));
			JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
			com.gathi.io.IO.writeDFtoTarget(outputJSONs, current, spark);
			System.exit(0);
			
		}*/
		// step 3: fetch current as DF & current pks; reorder 
		//.limit(5);
		
	//	current.show();
	
		
		long currentCount = current.count();
		// step 6: history and current columns
		String historyFields = String.join(", ", history.columns());
		String currentFields = String.join(", ", current.columns());
		
		// step 4: generate DF with md5 of pk & cdc for both history and current
		//history = generateDFWithMD5Fields(history, historyFieldsDict, spark);
		history = history.withColumn("pk_md5", functions.concat_ws("_", convertStringListToSeqTest(historyFieldsDict.get("pk_fields"))));
		history = history.withColumn("cdc_md5", functions.concat_ws("_", convertStringListToSeqTest(historyFieldsDict.get("cdc_fields"))));
		
		current = current.withColumn("pk_md5", functions.concat_ws("_", convertStringListToSeqTest(currentFieldsDict.get("pk_fields"))));
		current = current.withColumn("cdc_md5", functions.concat_ws("_", convertStringListToSeqTest(currentFieldsDict.get("cdc_fields"))));
	
		//current = generateDFWithMD5Fields(current, historyFieldsDict, spark);
		
		// step 4: rename DF fields with history and current prefix and register as temp views
	//	history = renameFieldsWithPrefix(history, "history_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
	//	current = renameFieldsWithPrefix(current, "current_", spark).repartition(10).persist(StorageLevel.MEMORY_AND_DISK_SER());
		
		history.show();
		current.show();
		System.out.println("\n=========================================== 1\n");
		
		
		history.createTempView("history");
		current.createTempView("current");
	 //   history.toDF(historyFieldsDict.get(""));
		// step 5: full outer join & register as temp view
	//	String joinStmt = "SELECT history_pk_md5, current_pk_md5, history_cdc_md5, current_cdc_md5 FROM history FULL OUTER JOIN current ON history.history_pk_md5 = current.current_pk_md5";
		String joinStmt = "SELECT history.pk_md5 AS history_pk_md5, current.pk_md5 AS current_pk_md5,"
				+ "history.cdc_md5 AS  history_cdc_md5, current.cdc_md5 AS current_cdc_md5 FROM history FULL OUTER JOIN current ON history.pk_md5 = current.pk_md5";
		
		Dataset<Row> joinedDF = spark.sql(joinStmt);
		joinedDF.persist(StorageLevel.MEMORY_ONLY());
		joinedDF.createTempView("joinedDF");
		//spark.sql("cache table joinedDF");
		
		joinedDF.show();
		System.out.println("\n=========================================== 2\n");
		//System.exit(99);

		
		
		
		String selectInserts = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_pk_md5 IS NULL";
		String selectDeletes = "SELECT history_pk_md5 AS pk_md5 FROM joinedDF where current_pk_md5 IS NULL";
		String selectUpdates = "SELECT current_pk_md5 AS pk_md5 FROM joinedDF where history_cdc_md5 <> current_cdc_md5";
	//	String selectMatches = "SELECT " + currentFields + ", 'M' as IUDM_INDICATOR FROM joinedDF where history_cdc_md5 = current_cdc_md5 ";
		
		Dataset<Row> inserts = spark.sql(selectInserts);
		Dataset<Row> deletes = spark.sql(selectDeletes);
		Dataset<Row> updates = spark.sql(selectUpdates);
	//	Dataset<Row> matches = spark.sql(selectMatches);
		
	//	inserts = removePrefix("current_", inserts, spark);
	//	deletes = removePrefix("history_", deletes, spark);
	//	updates = removePrefix("current_", updates, spark);
		
		inserts.createOrReplaceTempView("inserts");
		deletes.createOrReplaceTempView("deletes");
		updates.createOrReplaceTempView("updates");
		
		long insertFOJ = inserts.count();
		long deletesFOJ = deletes.count();
		long updatesFOJ = updates.count();
	//	inserts.show();
	//	deletes.show();
	//	updates.show();
		
		System.out.println("=================== ==================== ================ =================== =============== =============");
		System.out.println("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		System.out.println("=================== ==================== ================ =================== =============== =============");
		inserts = spark.sql("Select " + currentFields + ",'I' AS IUDM_INDICATOR FROM inserts left join current on inserts.pk_md5 = current.pk_md5");
		deletes = spark.sql("Select " + historyFields + ",'D' AS IUDM_INDICATOR FROM deletes left join history on deletes.pk_md5 = history.pk_md5");
		updates = spark.sql("Select " + currentFields + ",'U' AS IUDM_INDICATOR FROM updates left join current on updates.pk_md5 = current.pk_md5");
		
		
	/*	inserts.show();
		deletes.show();
		updates.show();*/
		// step 7: remove Prefix
		/*inserts = removePrefix("current_", inserts, spark);
		deletes = removePrefix("history_", deletes, spark);
		updates = removePrefix("current_", updates, spark);*/
		long insertCount = inserts.count();
		long deletsCount = deletes.count();
		long updateCound = updates.count();
		
		//step 8: union all
		Dataset<Row> output = inserts.union(deletes).union(updates);//.union(matches);
		
		//step 9: drop md5_pk md5_cdc fields
	//	output = output.drop("pk_md5");
	//	output = output.drop("cdc_md5");
		output = output.repartition(1);
		output.persist();
		long outputCount = output.count();
		System.out.println("\n\n\n\n =========================>>====================>>====================>>====================>>>>> "  );
		System.out.println("History count: " + histCount);
		System.out.println("Current count: " + currentCount);
		System.out.println("insertFOJ count: " + insertFOJ);
		System.out.println("deletesFOJ count: " + deletesFOJ);
		System.out.println("updatesFOJ count: " + updatesFOJ);
		System.out.println("insertCount count: " + insertCount);
		System.out.println("deletsCount count: " + deletsCount);
		System.out.println("updateCound count: " + updateCound);
	//	output.show();
		// step 10:  write data-frame to outputs
		JSONArray outputJSONs = deltaJSONConfig.getJSONArray("outputs");
		com.gathi.io.IO.writeDFtoTarget(outputJSONs, output, spark);
		
		return true;
	}
	
	private static Map<String, List<String>> getFieldsDict(JSONArray schema) {
		
		List<String> all_fields = new ArrayList<String>();
		List<String> pk_fields  = new ArrayList<String>();
		List<String> cdc_fields = new ArrayList<String>();
		
		JSONObject  field = null;
		String fieldName = null;
		for(int i = 0; i < schema.length(); i++) {		
			try {
				field = schema.getJSONObject(i);
				fieldName = field.getString("name");
				all_fields.add(fieldName);
				if(field.getString("is_pk").equalsIgnoreCase("true") || field.getString("is_pk").equalsIgnoreCase("y")){
					pk_fields.add(fieldName);
				}
				if(field.getString("is_cdc").equalsIgnoreCase("true") || field.getString("is_cdc").equalsIgnoreCase("y")) {
					cdc_fields.add(fieldName);
				}	
			}catch(Exception e) {
				LOG.error("invalid schema, missing fields");
				System.exit(4);
			}
			
		}
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();
		map.put("all_fields", all_fields);
		map.put("pk_fields", pk_fields);
		map.put("cdc_fields", cdc_fields);
		return map;
	}
	
private static Map<String, List<Column>> getFieldsDictNew(JSONArray schema) {
		
		List<Column> all_fields = new ArrayList<Column>();
		List<Column> pk_fields  = new ArrayList<Column>();
		List<Column> cdc_fields = new ArrayList<Column>();
		
		JSONObject  field = null;
		String fieldName = null;
		for(int i = 0; i < schema.length(); i++) {		
			try {
				field = schema.getJSONObject(i);
				fieldName = field.getString("name");
				all_fields.add(new Column(fieldName));
				if(field.getString("is_pk").equalsIgnoreCase("true") || field.getString("is_pk").equalsIgnoreCase("y")){
					pk_fields.add(new Column(fieldName));
				}
				if(field.getString("is_cdc").equalsIgnoreCase("true") || field.getString("is_cdc").equalsIgnoreCase("y")) {
					cdc_fields.add(new Column(fieldName));
				}	
			}catch(Exception e) {
				LOG.error("invalid schema, missing fields");
				System.exit(4);
			}
			
		}
		HashMap<String, List<Column>> map = new HashMap<String, List<Column>>();
		map.put("all_fields", all_fields);
		map.put("pk_fields", pk_fields);
		map.put("cdc_fields", cdc_fields);
		return map;
	}
	private static Dataset<Row> generateDFWithMD5Fields(Dataset<Row> df, Map<String, List<String>> dict, SparkSession spark){
		String pkMD5 = concatenateForMD5(dict.get("pk_fields"));
		String cdcMD5 = concatenateForMD5(dict.get("cdc_fields"));
		String allFields = String.join(", ", dict.get("all_fields"));
		String selectStmt = pkMD5 + " AS pk_md5,"
						+ cdcMD5 + " AS cdc_md5,"
						+ allFields;
		df.createOrReplaceTempView("temp");
		Dataset<Row> outDF = spark.sql("select " + selectStmt + " from temp");
		System.out.println("\n\n\n============================================\n" + selectStmt + "\n\n\n==========================================");
		return outDF;
	}
	
	private static String concatenateForMD5(List<String> fields){
		return "md5(" +  "concat_ws('_', " + castFieldsToStringStmt(fields) + ")" +  ")";
	}
	
	private static String castFieldsToStringStmt(List<String> fields){
		StringBuilder sb = new StringBuilder();
		for(String field : fields) {
			sb.append("CAST" + "(" + field + " AS STRING" + ")" + ","); 
		}
		String result = sb.toString();
		return result.length() > 0 ? result.substring(0, result.length() - 1) : "";
	}
	
	private static Dataset<Row> renameFieldsWithPrefix(Dataset<Row> df, String prefix, SparkSession spark){		
		StringBuilder sb = new StringBuilder();		
		for(String colName : df.columns()) {
			sb.append(colName + " AS " + prefix+colName + ",");
		}
		String selectFields = sb.toString();
		selectFields = selectFields.length() > 0 ? selectFields.substring(0, selectFields.length() - 1) : "";
		df.createOrReplaceTempView("temp");
		Dataset<Row> outDF = spark.sql("select " + selectFields + " from temp");
		return outDF;
	}
	
	private static Dataset<Row> removePrefix(String prefix, Dataset<Row> df, SparkSession spark){		
		StringBuilder sb = new StringBuilder();		
		for(String colName : df.columns()) {
			sb.append(colName + " AS " + colName.replace(prefix, "") + ",");
		}
		String selectFields = sb.toString();
		selectFields = selectFields.length() > 0 ? selectFields.substring(0, selectFields.length() - 1) : "";
		df.createOrReplaceTempView("temp");
		Dataset<Row> outDF = spark.sql("select " + selectFields + " from temp");
		return outDF;
	}
	public static Seq<Column> convertStringListToSeqTest(List<Column> in) {	    
	    return JavaConverters.asScalaIteratorConverter(in.iterator()).asScala().toSeq();
	}
}
