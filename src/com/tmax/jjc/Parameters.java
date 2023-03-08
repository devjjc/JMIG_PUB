package com.tmax.jjc;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

enum Key {
	// Common
	ID, CONN_URL, RULE_SCHEMA, RULE_PASS, USE_TPR, CREATE_TPR_SNAPSHOT_ALL
	, LOG_LEVEL, PRINT_CONSOLE
	// Data Migration
	, MIG_TABLE, MIG_TAB_RULE_NM, MIGTAB_PARALLEL_COUNT, IGNORE_MIG_TAB_ERROR, MIGTAB_START_MIGSEQ
	, MIGTOOL_NAME, MIGTOOL_HOME, MIGTOOL_LOGPATH
	, EXEC_NOLOGGING, USE_LONGTYPE_FOR_ROWCNT
	// Script Migration
	, MIG_SCRIPT, MIG_SCRIPT_RULE_NM, SCRIPT_HOME, IGNORE_MIG_SCRIPT_ERROR, SWITCH_OWNER_SCHEMA
	, SCRIPT_TABLE, TABLE_PARALLEL_COUNT, SCRIPT_INDEX, INDEX_PARALLEL_COUNT, INDEX_START_MIGSEQ, INDEX_CHK_DEPENDENCY, INDEX_CHK_DEPENDENCY_FOR_PARTITION
	, SCRIPT_CONSTRAINT, CONSTRAINT_PARALLEL_COUNT, CONSTRAINT_CHK_DEPENDENCY, SCRIPT_ETC, ETC_PARALLEL_COUNT
	// Gather Stat
	, GATHER_STAT, STAT_RULE_NM, STAT_PARALLEL_COUNT, STAT_START_MIGSEQ, STAT_CHK_DEPENDENCY
	
    
}

public class Parameters {

	// public Key key;
	private Logger logger;
	
	Parameters(Logger logger) {
		this.logger = logger;
	}

	public Properties initParams(String[] args) {
		Properties Props = new Properties();
		
		// Common
		Props.put(Key.ID.name(), "jmig");
		Props.put(Key.CONN_URL.name(), "jdbc:tibero:thin:@localhost:8629:tibero");
		Props.put(Key.RULE_SCHEMA.name(), "jmig");
		Props.put(Key.RULE_PASS.name(), "jmig");
		Props.put(Key.USE_TPR.name(), "N");
		Props.put(Key.CREATE_TPR_SNAPSHOT_ALL.name(), "N");
		
		// log
		Props.put(Key.LOG_LEVEL.name(), "INFO");
		Props.put(Key.PRINT_CONSOLE.name(), "Y");
		
		// data migration
		Props.put(Key.MIG_TABLE.name(), "Y");
		Props.put(Key.MIG_TAB_RULE_NM.name(), "MIG_TAB");
		Props.put(Key.MIGTAB_PARALLEL_COUNT.name(), "4");
		Props.put(Key.IGNORE_MIG_TAB_ERROR.name(), "Y");
		Props.put(Key.MIGTAB_START_MIGSEQ.name(), "0");
		Props.put(Key.MIGTOOL_NAME.name(), "migrator.sh");
		Props.put(Key.MIGTOOL_HOME.name(), "/home/tibero/work/jmig/migrator");
		Props.put(Key.MIGTOOL_LOGPATH.name(), "/home/tibero/work/jmig/migrator/auto_log");
		Props.put(Key.EXEC_NOLOGGING.name(), "N");
		Props.put(Key.USE_LONGTYPE_FOR_ROWCNT.name(), "N");
		Props.put(Key.MIG_SCRIPT.name(), "Y");
		Props.put(Key.MIG_SCRIPT_RULE_NM.name(), "MIG_SCRIPT");
		Props.put(Key.SCRIPT_HOME.name(), "/home/tibero/work/jmig/script");
		Props.put(Key.IGNORE_MIG_SCRIPT_ERROR.name(), "Y");
		Props.put(Key.SWITCH_OWNER_SCHEMA.name(), "N");
		
		// script migration
		Props.put(Key.SCRIPT_TABLE.name(), "N");
		Props.put(Key.TABLE_PARALLEL_COUNT.name(), "1");		
		Props.put(Key.SCRIPT_INDEX.name(), "Y");
		Props.put(Key.INDEX_PARALLEL_COUNT.name(), "4");
		Props.put(Key.INDEX_START_MIGSEQ.name(), "0");
		Props.put(Key.INDEX_CHK_DEPENDENCY.name(), "N");
		Props.put(Key.INDEX_CHK_DEPENDENCY_FOR_PARTITION.name(), "N");
		Props.put(Key.SCRIPT_CONSTRAINT.name(), "N");
		Props.put(Key.CONSTRAINT_PARALLEL_COUNT.name(), "4");
		Props.put(Key.CONSTRAINT_CHK_DEPENDENCY.name(), "N");
		Props.put(Key.SCRIPT_ETC.name(), "N");
		Props.put(Key.ETC_PARALLEL_COUNT.name(), "1");
		
		
		// gather stat
		Props.put(Key.GATHER_STAT.name(), "N");
		Props.put(Key.STAT_RULE_NM.name(), "MIG_STAT");
		Props.put(Key.STAT_PARALLEL_COUNT.name(), "4");	
		Props.put(Key.STAT_START_MIGSEQ.name(), "0");
		Props.put(Key.STAT_CHK_DEPENDENCY.name(), "Y");	
		
		
		// read property file
		String propertyFile = args[0];
		FileReader reader = null;			
		boolean isErr = false;
		
		try {
			File f = new File(propertyFile);
			if (f.exists()) {
				logger.info("founds property file");
				reader = new FileReader(f);
				Props.load(reader);
			} else {
				logger.error("property file was not found : " + f.getAbsolutePath());
				isErr = true;
			}

		} catch (Exception err1) {
			logger.error("error in  parsing property file");
			logger.error(err1.getMessage());
			
			isErr = true;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception err2) {
					logger.error(err2.getMessage());
					// MLogger.ERROR(var18.getMessage());
				}
			}

		}
		
		// 디렉토리 관련 파라미터에서 마지막 경로부분 존재 시 삭제처리
		Props.setProperty("MIGTOOL_HOME", this.ParamDelSlash(Props.getProperty("MIGTOOL_HOME")) );
		Props.setProperty("MIGTOOL_LOGPATH", this.ParamDelSlash(Props.getProperty("MIGTOOL_LOGPATH")) );
		Props.setProperty("SCRIPT_HOME", this.ParamDelSlash(Props.getProperty("SCRIPT_HOME")) );
		
		/* Print Parameters */
		logger.info("=======================================================");
		for(Key key : Key.values()) {
			if(Key.ID.name().equals(key.toString())) {
				logger.info("#### COMMON ####");
			} else if(Key.LOG_LEVEL.name().equals(key.toString())) {
				logger.info("");
				logger.info("#### LOG ####");				
			} else if(Key.MIG_TABLE.name().equals(key.toString())) {
				logger.info("");
				logger.info("#### Data Migration ####");
			} else if(Key.MIGTOOL_NAME.name().equals(key.toString())) {
				logger.info("# table migrator");
			} else if(Key.EXEC_NOLOGGING.name().equals(key.toString())) {
				logger.info("# dblink");
			} else if(Key.MIG_SCRIPT.name().equals(key.toString())) {
				logger.info("");
				logger.info("#### Script Migration ####");
			} else if(Key.SCRIPT_TABLE.name().equals(key.toString())) {
				logger.info("# table");
			} else if(Key.SCRIPT_INDEX.name().equals(key.toString())) {
				logger.info("# index");
			} else if(Key.SCRIPT_CONSTRAINT.name().equals(key.toString())) {
				logger.info("# constraint");
			} else if(Key.SCRIPT_ETC.name().equals(key.toString())) {
				logger.info("# etc");
			} else if(Key.GATHER_STAT.name().equals(key.toString())) {
				logger.info("");
				logger.info("#### Gather Stat ####");
			} else if(Key.RULE_PASS.name().equals(key.toString())) {
				// 패스워크 별표 처리
				logger.info(String.format("%s=%s", key.toString(), "****"));
				continue;
			}
			logger.info(String.format("%s=%s", key.toString(), Props.getProperty(key.toString())));
		}
		logger.info("=======================================================");
		
		return isErr ? null : Props;
	}

	public String ParamDelSlash(String dir) {
		String ret = null;
		if( dir != null && dir.lastIndexOf("/") == dir.length()-1 ) {
			ret = dir.substring(0, dir.length()-1);
		} else {
			ret = dir;
		}

		return ret;
	}
	
	public boolean paramCheck(Properties Props) {
		/* @ 파라미터 파일 입력값 적정성 체크 , 로직구현 필요 */
		boolean ret = true;
		String tmp = null;
		//File fTmp = null;
		
		logger.info("Start Parameter Check");
		
		// MIG_TABLE=N && MIG_INDEX=N CASE
		if( "N".equalsIgnoreCase(Props.getProperty("MIG_TABLE")) 
				&& "N".equalsIgnoreCase(Props.getProperty("MIG_SCRIPT") ) 
				&& "N".equalsIgnoreCase(Props.getProperty("GATHER_STAT") ) ) {
			logger.error("[PARAM] Job Not Found - Check MIG_TABLE/MIG_SCRIPT/GATHER_STAT");
			ret = false;
		}
		
		if("Y".equalsIgnoreCase(Props.getProperty("MIG_TABLE"))) {
			
			// MIGTOOL_LOGPATH 존재여부
	        if (isExist(Props.getProperty("MIGTOOL_LOGPATH"), true) ) {
	        	logger.trace("[PARAM] dir exists - MIGTOOL_LOGPATH OK");
	        } else {
	        	logger.error("[PARAM] dir not exitst - Check MIGTOOL_LOGPATH");
	        	ret = false;
	        }
	        
	        
	        // MIGTOOL_HOME 파일명 존재여부
	        tmp = Props.getProperty("MIGTOOL_HOME")+"/"+Props.getProperty("MIGTOOL_NAME");
	        // 아래로직 사전처리 완료
//	        if(tmp.lastIndexOf("/") == tmp.length()-1 ) {
//	        	tmp += Props.getProperty("MIGTOOL_NAME");
//	        } else {
//	        	tmp += "/"+Props.getProperty("MIGTOOL_NAME");
//	        }
	        
	        if (isExist(tmp, false) ) {
	        	logger.trace("[PARAM] file exists - MIGTOOL_HOME/MIGTOOL_NAME OK");
	        } else {
	        	logger.error("[PARAM] file not exitst - Check MIGTOOL_HOME/MIGTOOL_NAME");
	        	ret = false;
	        }
			
			// MIGTAB_PARALLEL_COUNT 10000개 초과여부
	        tmp = Props.getProperty("MIGTAB_PARALLEL_COUNT");
	        if (tmp == null || Integer.parseInt(tmp) >= 1000 )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check MIGTAB_PARALLEL_COUNT");
	        	ret = false;
	        }
	        
		} // Table Migration
		
		
		if(ret && "Y".equalsIgnoreCase(Props.getProperty("MIG_SCRIPT"))) {
			
			// MIG_TABLE=N && MIG_INDEX=N CASE
			if( "N".equalsIgnoreCase(Props.getProperty("SCRIPT_INDEX")) 
					&& "N".equalsIgnoreCase(Props.getProperty("SCRIPT_TABLE"))
							&& "N".equalsIgnoreCase(Props.getProperty("SCRIPT_CONSTRAINT"))
								&& "N".equalsIgnoreCase(Props.getProperty("SCRIPT_ETC")) ){
				logger.error("[PARAM] Script Job Not Found - Check SCRIPT_TABLE/SCRIPT_INDEX/SCRIPT_CONSTRAINT/SCRIPT_ETC");
				ret = false;
			}
			
			
			// MIGIDX_PARALLEL_COUNT 10000개 초과여부
	        tmp = Props.getProperty("INDEX_PARALLEL_COUNT");
	        if ( (tmp == null || Integer.parseInt(tmp) >= 1000 ) && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_INDEX")) )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check INDEX_PARALLEL_COUNT");
	        	ret = false;
	        }
			
	        tmp = Props.getProperty("TABLE_PARALLEL_COUNT");
	        if ( (tmp == null || Integer.parseInt(tmp) >= 1000 ) && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_TABLE")) )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check TABLE_PARALLEL_COUNT");
	        	ret = false;
	        }
	        
	        tmp = Props.getProperty("CONSTRAINT_PARALLEL_COUNT");
	        if ( (tmp == null || Integer.parseInt(tmp) >= 1000 ) && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_CONSTRAINT")) )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check CONSTRAINT_PARALLEL_COUNT");
	        	ret = false;
	        }
	        
	        tmp = Props.getProperty("ETC_PARALLEL_COUNT");
	        if ( (tmp == null || Integer.parseInt(tmp) >= 1000 ) && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_ETC")) )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check ETC_PARALLEL_COUNT");
	        	ret = false;
	        }
	        
	        tmp = Props.getProperty("STAT_PARALLEL_COUNT");
	        if ( (tmp == null || Integer.parseInt(tmp) >= 1000 ) && "Y".equalsIgnoreCase(Props.getProperty("GATHER_STAT")) )  {
	        	logger.error("[PARAM] not setting OR not exceed 1000 - Check STAT_PARALLEL_COUNT");
	        	ret = false;
	        }
	        
	        
			// SCRIPT_HOME 존재여부
	        if (isExist(Props.getProperty("SCRIPT_HOME"), true) ) {
	        	logger.trace("[PARAM] dir exists - SCRIPT_HOME OK");
	        } else {
	        	logger.error("[PARAM] dir not exitst - Check SCRIPT_HOME");
	        	ret = false;
	        }
	        
		} // Index Migration
		
		return ret;
	} // paramCheck
	
	private boolean isExist(String name, boolean checkDir) {
		boolean ret = true;	
		
		if(name == null ) {
			ret = false;
		}
		
		if(ret) {
			File file = new File(name);
			
	        if (file.exists()) {
	        	// 디렉토리 추가 체크
	        	if(checkDir) {
	        		if(!file.isDirectory()) {
	        			ret = false;
	        		}
	        	}
	        	
	        } else {
	        	ret = false;
	        }
		}
		
		

		return ret;
	}
	
}
