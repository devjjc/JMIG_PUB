package com.tmax.jjc;

import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.LoggerConfig;

/* 
 * �����̷�
 * v1.0 :  ������
 * v1.1 : �Ķ�����߰� - INDEX_CHK_DEPENDENCY, CONSTRAINT_CHK_DEPENDENCY, CREATE_TPR_SNAPSHOT_ALL, Rule Table(������) - part_type, part_name �÷��߰�
 * v1.2 : ������ ����߰�(GATHER_STAT=Y) DISABLED�� �������� Ȱ��ȭ ��� �߰�
 * v1.3 : �������̰� �� link_mode �÷��߰�, status �ʱⰪ ��READY�� ����, �ε����̰� �� ������ ����߰�, �Ķ���� �߰� - MIGTAB_START_MIGSEQ, INDEX_CHK_DEPENDENCY_FOR_PARTITION
 * v1.4 : ���ǰ������� �� �翬�� ����߰�
 * v1.5 : �Ķ���� �߰� - INDEX_START_MIGSEQ, STAT_START_MIGSEQ
 * v1.6 : �Ķ���� �߰� - USE_LONGTYPE_FOR_ROWCNT, db link�� Statement.executeLargeUpdate() ����Ͽ� row count �� long Ÿ�� ����
 */
public class JMigrate {
	final static String INDEX = "INDEX";
	final static String CONSTRAINT = "CONSTRAINT";
	final static String TABLE = "TABLE";
	final static String ETC = "ETC";
	
	public static void main(String[] args) {

		/* @ �Է°� ó��, �Ķ�������� Ȯ�� */

		if (args != null) {

			if (args.length != 1) {
				System.out.println("Usage : PROGRAM_NAME PROPERTY_FILE_NAME");
				return;
			}
			
			if ("-v".equalsIgnoreCase(args[0]) || "-version".equalsIgnoreCase(args[0]) ) {
				System.out.println("J-Migration Version : 1.6");
				return;
			}

//			for (int i = 0; i < args.length; i++) {
//				System.out.println("args["+i+"] : " + args[i]);				
//			}
		}
		
		/* Log ó�� */
		Logger logger = LogManager.getLogger(JMigrate.class);
		
		Parameters param = new Parameters(logger);
		Properties Props = param.initParams(args);

		// ���� �ܰ� ���࿩�� üũ
		if(Props == null ) {
			return;
		}
		
		


		/* Log ���� ó�� */		
		switch(Props.getProperty("LOG_LEVEL").toUpperCase()) {
		case "TRACE" : 
//			if(printConsole!=null && "Y".equals(printConsole)) { Configurator.setLevel("com.tmax.jjc", Level.TRACE); }
//			Configurator.setLevel("com.tmax.jjc", Level.TRACE);
//			setLevel(logger, Level.TRACE);
			Configurator.setRootLevel(Level.TRACE);
			break;
		case "DEBUG" : 
			Configurator.setRootLevel(Level.DEBUG);
			break;			
		case "ERROR" : 
			Configurator.setRootLevel(Level.ERROR);
			break;
		default :
			Configurator.setRootLevel(Level.INFO);
		}
		
		// �ַܼα� ��¿��� ����
		String printConsole = Props.getProperty("PRINT_CONSOLE").toUpperCase();
		if(printConsole!=null && "N".equals(printConsole)) {
			logger.info("Console Log OFF");
		    final LoggerContext context = (LoggerContext) LogManager.getContext(false);
		    final Configuration config = context.getConfiguration();
		    LoggerConfig loggerConfig = config.getRootLogger();
		    loggerConfig.removeAppender("Console");
		    context.updateLoggers();
		};
		
		/* @ �Ķ���� ���� �Է°� ������ üũ */
		boolean isStop = false;
		
		if(!param.paramCheck(Props)) {
			isStop = true;
		}
		
		/* ���μ��� ID ó��, .proc.list */
		JFile jf = new JFile(logger);
		jf.proc_list(".proc.list");

		

		/* Table Script Start! */
		// ���̺�ũ��Ʈ ��� ���� ���� ó��
		if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("MIG_SCRIPT")) && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_TABLE")) ) {
			
			String script_type = null;
			
			/* Table Script */
			logger.info("(JMIG) Table Script Start!!");
			
			script_type = TABLE;
			int thread_cnt = Integer.parseInt(Props.getProperty("TABLE_PARALLEL_COUNT"));
			Thread[] thr = new Thread[thread_cnt];
			
			for(int i=0; i<thread_cnt; i++) {
				thr[i] = new Thread(new ThreadGroupScriptMig(Props, script_type, logger), String.format("TAB-%04d", i+1) );
				thr[i].start();
			}
			
			// migration thread ���
			for(int i=0; i<thread_cnt; i++) {
				try {
					thr[i].join();
				} catch (InterruptedException e) {
					logger.error("error in waiting TABLE script mig thread join");
					logger.error(e.getMessage());
					isStop = true;
				}
			}
			
			logger.info("(JMIG) Table Script End!!");
			
			
		} // Table Script End
		

		/* init TPR Connection */
		TPR tpr = new TPR(Props.getProperty("CREATE_TPR_SNAPSHOT_ALL"), logger);
		boolean isTpr = false;
		
		if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
			tpr.initConn(Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
			isTpr = true;
		}
		
		/* Migration Start! */
		if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("MIG_TABLE"))) {
			
			// ���̱׷��̼� ����
			logger.info("(JMIG) Table Data Migration Start!!");
			
			/* Create TPR SnapShot */
			if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
				tpr.createTPRSnapshot("Data Migration Start");
			}
			
			int thread_cnt = Integer.parseInt(Props.getProperty("MIGTAB_PARALLEL_COUNT"));
			
			
			/* �ϴ� ������ ������ �ð� �� �� �߰� ���� �ʿ� */
			Thread[] thr = new Thread[thread_cnt];
			
			for(int i=0; i<thread_cnt; i++) {
				// Thread t = new Thread(new ThreadGroupMig(Props, logger), String.format("MIG-%04d", i+1) );
				thr[i] = new Thread(new ThreadGroupMig(Props, logger), String.format("MIG-%04d", i+1) );
				thr[i].start();
			}
			
			// migration thread ���
			for(int i=0; i<thread_cnt; i++) {
				try {
					thr[i].join();
				} catch (InterruptedException e) {
					logger.error("error in waiting mig table data thread join");
					logger.error(e.getMessage());
					isStop = true;
				}
			}
			
			logger.info("(JMIG) Table Data Migration End!!");
			
		} // Table Migration
		

		
		/* Script Start! */
		if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("MIG_SCRIPT"))) {
			
			logger.info("(JMIG) Script Migration Start!!");
			String script_type = null;
			
			/* Index Start */
			if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_INDEX")) ) {
				/* Index Script */
				logger.info("(JMIG) Index Script Start!!");
				
				/* Create TPR SnapShot */
				if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
					tpr.createTPRSnapshot("Index Migration Start");
				}
				
				
				script_type = INDEX;
				int thread_cnt = Integer.parseInt(Props.getProperty("INDEX_PARALLEL_COUNT"));
				Thread[] thr = new Thread[thread_cnt];
				
				for(int i=0; i<thread_cnt; i++) {
					thr[i] = new Thread(new ThreadGroupScriptMig(Props, script_type, logger), String.format("IDX-%04d", i+1) );
					thr[i].start();
				}
				
				// migration thread ���
				for(int i=0; i<thread_cnt; i++) {
					try {
						thr[i].join();
					} catch (InterruptedException e) {
						logger.error("error in waiting INDEX script mig thread join");
						logger.error(e.getMessage());
						isStop = true;
					}
				}
				
				logger.info("(JMIG) Index Script End!!");
			} // Index End

			/* Constraint Start */
			if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_CONSTRAINT")) ) {
				/* Constraint Script */
				logger.info("(JMIG) Constraint Script Start!!");
				
				/* Create TPR SnapShot */
				if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
					tpr.createTPRSnapshot("Constraint Migration Start");
				}
				
				script_type = CONSTRAINT;
				int thread_cnt = Integer.parseInt(Props.getProperty("CONSTRAINT_PARALLEL_COUNT"));
				Thread[] thr = new Thread[thread_cnt];
				
				for(int i=0; i<thread_cnt; i++) {
					thr[i] = new Thread(new ThreadGroupScriptMig(Props, script_type, logger), String.format("CON-%04d", i+1) );
					thr[i].start();
				}
				
				// migration thread ���
				for(int i=0; i<thread_cnt; i++) {
					try {
						thr[i].join();
					} catch (InterruptedException e) {
						logger.error("error in waiting CONSTRAINT script mig thread join");
						logger.error(e.getMessage());
						isStop = true;
					}
				}
				
				logger.info("(JMIG) Constraint Script End!!");
			} // constraint end
			
			
			/* ETC Start */
			if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("SCRIPT_ETC")) ) {
				/* Table Script */
				logger.info("(JMIG) ETC Script Start!!");
				
				/* Create TPR SnapShot */
				if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
					tpr.createTPRSnapshot("ETC Migration Start");
				}
				
				script_type = ETC;
				int thread_cnt = Integer.parseInt(Props.getProperty("ETC_PARALLEL_COUNT"));
				Thread[] thr = new Thread[thread_cnt];
				
				for(int i=0; i<thread_cnt; i++) {
					thr[i] = new Thread(new ThreadGroupScriptMig(Props, script_type, logger), String.format("ETC-%04d", i+1) );
					thr[i].start();
				}
				
				// migration thread ���
				for(int i=0; i<thread_cnt; i++) {
					try {
						thr[i].join();
					} catch (InterruptedException e) {
						logger.error("error in waiting ETC script mig thread join");
						logger.error(e.getMessage());
						isStop = true;
					}
				}
				
				logger.info("(JMIG) ETC Script End!!");
			} // ETC End
			
			
			logger.info("(JMIG) Script Migration End!!");

			
		} // Script End
		
		
		/* Stat Start! */
		if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("GATHER_STAT"))) {
			
			// ���̱׷��̼� ����
			logger.info("(JMIG) Gathering Stat Start!!");
			
			/* Create TPR SnapShot */
			if(!isStop && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
				tpr.createTPRSnapshot("Stat Start");
			}
			
			int thread_cnt = Integer.parseInt(Props.getProperty("STAT_PARALLEL_COUNT"));
			
			
			/* �ϴ� ������ ������ �ð� �� �� �߰� ���� �ʿ� */
			Thread[] thr = new Thread[thread_cnt];
			
			for(int i=0; i<thread_cnt; i++) {
				thr[i] = new Thread(new ThreadGroupStat(Props, logger), String.format("STAT-%04d", i+1) );
				thr[i].start();
			}
			
			// migration thread ���
			for(int i=0; i<thread_cnt; i++) {
				try {
					thr[i].join();
				} catch (InterruptedException e) {
					logger.error("error in waiting gathering stat thread join");
					logger.error(e.getMessage());
					isStop = true;
				}
			}
			
			logger.info("(JMIG) Gathering Stat End!!");
			
		} // Stat End
		
		
		/* Create TPR SnapShot */
		// ������ ������
		if(isTpr && "Y".equalsIgnoreCase(Props.getProperty("USE_TPR")) ) {
			tpr.createTPRSnapshot("Last TPR SnapShot");
			tpr.closeConn();
		}

		

	} // main
	

} // JMigrate End
