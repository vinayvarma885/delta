package com.gathi.mdm.delta;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class DeltaLogger {

	public static void configureLogger(String logFilePath){
		Logger.getRootLogger().getLoggerRepository().resetConfiguration();
		
		ConsoleAppender console = new ConsoleAppender();
		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
		console.setLayout(new PatternLayout(PATTERN)); 
		console.setThreshold(Level.ERROR);
		console.setTarget("System.err");
		console.activateOptions();
		  //add appender to any Logger (here is root)
		Logger.getRootLogger().addAppender(console);	
		
		FileAppender file = new FileAppender();
		file.setLayout(new PatternLayout(PATTERN)); 
		file.setThreshold(Level.ERROR);
		file.setFile(logFilePath);
		file.activateOptions();
		
		Logger.getRootLogger().addAppender(file);
		
		FileAppender infofile = new FileAppender();
		infofile.setLayout(new PatternLayout(PATTERN)); 
		infofile.setThreshold(Level.INFO);
		infofile.setFile(logFilePath+".info");
		infofile.activateOptions();
		
		Logger.getRootLogger().addAppender(infofile);
	}
}
