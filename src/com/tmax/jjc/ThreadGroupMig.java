package com.tmax.jjc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.Logger;

import com.tmax.tibero.jdbc.ext.TbDataSource;

public class ThreadGroupMig implements Runnable {

	final String TOOL = "MIGRATOR";
	final String LINK = "DBLINK";
	
	private Properties Props = null;
	private Logger Log = null;
	
	public ThreadGroupMig(Properties p, Logger l) {
		Props = p;
		Log = l;
	}
	
	@Override
	
	public void run() {
		
		
		TbDataSource tds = new TbDataSource();
		Connection conn = null;
		boolean isStop = false;
		DBCheck dbc = new DBCheck(Log);
		
		TabMig tab = new TabMig(Props.getProperty("MIG_TAB_RULE_NM"), Integer.parseInt(Props.getProperty("MIGTAB_START_MIGSEQ")), Log);

		try {
			
			tds.setURL(Props.getProperty("CONN_URL"));
			tds.setUser(Props.getProperty("RULE_SCHEMA"));
			tds.setPassword(Props.getProperty("RULE_PASS"));
			conn = tds.getConnection();
			conn.setAutoCommit(false);
			
			/* 1. ���̱׷��̼� �����ȸ */
			while(true) {
				int iret = tab.getMigTabInfo(conn);
				
				switch(iret) {
				case 1: // �̰� ������
					Log.info("Not Exists Migration Table");
					isStop = true;
					break;
				case 2: // �̰���� ����, ����ȸ �ʿ�
					Log.trace("repeat getMigTabInfo");
					if(tab.getMigTabInfo(conn) != 0) {
						Log.error("repeat Fail");
						isStop = true;
					} else {
						Log.debug("repeat select success");
					}
					break;
				case 100:  // �����߻�
					Log.error("getMigTabInfo Error!!");
					isStop = true;
					break;
				}
				
				// ���� �ܰ� ���࿩�� üũ
				if(isStop) {
					break;
				}
			
				
				/* 2. ���̱׷��̼� ��� lock */

				if (!tab.setMigTabLock(conn)) {
					Log.debug("setMigTabLock Fail, Repeat Step[1]");
					
					// select ~ for update ����, ���ü��� ���Ͽ� ���ɼ��� �����Ƿ� 1���̵� �ʿ�
					continue;

				}
				

				/* 3. �̰����� �� �۾� */
				if( !tab.uptPreMigInfo(conn, Props.getProperty("ID")+"_"+Thread.currentThread().getName() )) { 
					
					// select ~ for update ���� update ���а� �߻��ϴ� ������ �߻��ϸ� �ȵǴ� ������ ����
					Log.error("uptPreMigInfo Fail, update count is 0");

					//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					tab.setErr(conn, "ERR", "uptPreMigInfo Fail : update count is 0");
					
					// ���� �߻� �� �ٽ� �������� �̵�
					continue;
				
				}
				
				/* 4. migrator/dblink ���� */
				String mig_type = tab.getMigType();
				TabMigRst mig_rst = null;
				
				if(mig_type != null && this.TOOL.equalsIgnoreCase(mig_type) ) { // table migrator
					
					if (!tab.migrator(Props.getProperty("MIGTOOL_NAME"), Props.getProperty("MIGTOOL_HOME"), Props.getProperty("MIGTOOL_LOGPATH"))) {
						// �������� �ʿ�
						Log.error("migrator tool Fail!");
						//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
						tab.setErr(conn, "ERR", "4. migrator tool fail");
						
						// �����̺� ������� ���� �����ʿ�!!!
						isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					}
					
					// ���� �ܰ� ���࿩�� üũ
					if( isStop ) {
						break;
					}
	
					/* 5. �α����� �м���� ���� */
					mig_rst = tab.miglog_analyzer(Props.getProperty("MIGTOOL_LOGPATH"));
					 
					/* migrator�� ����Ǿ��ٸ�, �α� ��� �κп����� ���� ���ɼ��� �־, 
					 * ����ó�� skip �Ͽ���, 6�ܰ迡�� ���ڰ��� ���� ���·� ���� üũ���� 
					 */
					
				} else if (mig_type != null && this.LINK.equalsIgnoreCase(mig_type) ) { // dblink
					
					mig_rst = tab.dblink(conn, Props.getProperty("EXEC_NOLOGGING"), Props.getProperty("USE_LONGTYPE_FOR_ROWCNT"));
					
				} else { // unknown type
					Log.error("unknown mig_type!");
					//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					tab.setErr(conn, "ERR", "unknown mig_type!");
					isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					
					// ���� �ܰ� ���࿩�� üũ
					if( isStop ) {
						break;
					}
					
				}
				
				 /* 6. �̰����� �� �۾� */
				 if( !tab.uptPostMigInfo(conn, mig_rst.getRowCnt(), mig_rst.getElapsedMs(), mig_rst.getVerify() )) {
					 
					 Log.error("uptPostMigInfo Fail!");
					 
					 // tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					 
					 // �۾� �� update ���� ����, ������� �ʿ�
					 tab.setErr(conn, "ERR", "6. uptPostMigInfo Fail!");
					 
					 isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					 
					 /* Connection ��ȿ�� üũ */
					 // �̰����̰ų�, ��ó������ ����ɶ��� ȿ���� ����, �Ź� üũ���� �ʱ� ����
					 iret = dbc.isValidConn(conn);
					 
					 if(iret == 1) { // reconnect
						tds.setURL(Props.getProperty("CONN_URL"));
						tds.setUser(Props.getProperty("RULE_SCHEMA"));
						tds.setPassword(Props.getProperty("RULE_PASS"));
						conn = tds.getConnection();
						conn.setAutoCommit(false);
						
						Log.info("Reconnect DB");
						tab.setErr(conn, "ERR", "Abnormal Session Close!");
						 
					} else if (iret == 2 || iret == -1) { // error
						Log.error("iret : " + iret);
						isStop = true;
					}
					 
				 }
				 
				 
				 // ����ó�� �Ϸ�
				 if(isStop) {
					 Log.info("Stop Work Thread : Data Migration");
					 break;
				 } else {
					 Log.info("Unit Data Migration Complete!");
				 }
			} // main while end

		} catch (SQLException e) {
			Log.error(e.getMessage());

		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				Log.error(e.getMessage());
			}
		} // finally
		
		
	} // run end

} // class end
	


