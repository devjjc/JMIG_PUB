package com.tmax.jjc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;

import org.apache.logging.log4j.Logger;

public class JFile {
	
	private Logger logger;

	public JFile(Logger logger) {
		this.logger = logger;
	}
	
	public boolean proc_list(String name) {
		boolean ret = true;	
		
		String pid = ManagementFactory.getRuntimeMXBean().getName();
		String processID = pid.substring(0, pid.indexOf("@"));
		logger.info("Main Process ID (PID) : " + pid);
		
		try {
			FileWriter fw;
			fw = new FileWriter(name, false);
			
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(processID);
			bw.close();
		} catch (IOException  e) {
			// e.printStackTrace();
			logger.warn("error in proc_list(start)");
			logger.warn(e.getMessage());
			ret = false;
		}
		
		return ret;
	}


}
