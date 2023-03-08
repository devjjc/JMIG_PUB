package com.tmax.jjc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.Logger;

import com.tmax.tibero.jdbc.ext.TbDataSource;

public class ThreadGroupScriptMig implements Runnable {

	final String CONSTRAINT = "CONSTRAINT";
	final String INDEX = "INDEX";
	final String DDEnb = "DD_ENABLE";       // enable constraint
	final String DDEnbNo = "DD_ENABLE_NO"; // enable novalidate constraint
	final String DDRebuild = "DD_REBUILD"; // index rebuild
	
	private Properties Props = null;
	private Logger Log = null;
	private String Type = null;
	
	
	public ThreadGroupScriptMig(Properties p, String script_type, Logger l) {
		Props = p;
		Log = l;
		Type = script_type;
	}
	
	@Override
	
	public void run() {
		
		
		TbDataSource tds = new TbDataSource();
		Connection conn = null;
		
		DBCheck dbc = new DBCheck(Log);
		
		ScriptMig script = new ScriptMig(Props.getProperty("MIG_SCRIPT_RULE_NM"), Props.getProperty("MIG_TAB_RULE_NM"), 
				Props.getProperty("INDEX_CHK_DEPENDENCY"), Props.getProperty("INDEX_CHK_DEPENDENCY_FOR_PARTITION")
				, Props.getProperty("CONSTRAINT_CHK_DEPENDENCY"), Integer.parseInt(Props.getProperty("INDEX_START_MIGSEQ")), Log);

		try {
			tds.setURL(Props.getProperty("CONN_URL"));
			tds.setUser(Props.getProperty("RULE_SCHEMA"));
			tds.setPassword(Props.getProperty("RULE_PASS"));
			conn = tds.getConnection();
			conn.setAutoCommit(false);
			
			// ���κ���
			int iret = 0;
			boolean isStop = false;
			
			/* 1. ��ũ��Ʈ �����ȸ */
			while(true) {
				// ������ �ʱ�ȭ
				isStop = false;
				
				// connection �ʱ�ȭ ����ʿ�
				
				iret = script.getMigScriptInfo(conn, this.Type);
				switch(iret) {
				case 1: // �̰� ������
					Log.info("Not Exists Migration Script");
					isStop = true;
					break; 
				case 2: // �̰���� ����, ����ȸ �ʿ�
					Log.trace("repeat getMigScriptInfo");
					if(script.getMigScriptInfo(conn, this.Type) != 0) {
						Log.error("repeat Fail");
						isStop = true;
					} else {
						Log.debug("repeat select success");
					}
					break;
				case 100:  // �����߻�
					Log.error("getMigScriptInfo Error!!");
					isStop = true;
					break;
				}
				
				// ���� �ܰ� ���࿩�� üũ
				if(isStop) {
					break;
				}
			
				/* 2. ��ũ��Ʈ ��� lock */
				iret = script.setMigScriptLock(conn, this.Type);
				if ( iret == 0) {
					Log.debug("setMigScriptLock Fail, Repeat Step[1]");
					
					// select ~ for update ����, ���ü��� ���Ͽ� ���ɼ��� �����Ƿ� 1���̵� �ʿ�
					continue;

				} else if(iret == 1 ) { // index && idx_cnt =1 ó��, table, constraint, etc ó��
					/* 3. ��ũ��Ʈ �� �۾� */
					if( !script.uptPreMigInfo(conn, this.Type, Props.getProperty("ID")+"_"+Thread.currentThread().getName(), 0 )) { 
						
						// select ~ for update ���� update ���а� �߻��ϴ� ������ �߻��ϸ� �ȵǴ� ������ ����
						Log.error("uptPreMigInfo Fail, update count is 0");
						script.setErr(conn, this.Type, "ERR", "uptPreMigInfo Fail : update count is 0");
						
						continue;
					}
					
					/* �� �۾� ó�� */
					if(mig_script(script, conn)) {
						Log.info("Unit Script Migration Complete : " + this.Type);
					} else {
						
						Log.warn("Unit Script Migration Not Complete : " + this.Type);
						
						 /* Connection ��ȿ�� üũ */
						// �̰��� ����ɶ��� ȿ���� ����, �Ź� üũ���� �ʱ� ����
						iret = dbc.isValidConn(conn);
						if(iret == 1) { // reconnect
							tds.setURL(Props.getProperty("CONN_URL"));
							tds.setUser(Props.getProperty("RULE_SCHEMA"));
							tds.setPassword(Props.getProperty("RULE_PASS"));
							conn = tds.getConnection();
							conn.setAutoCommit(false);
							
							Log.info("Reconnect DB");
							script.setErr(conn, this.Type, "ERR", "Abnormal Session Close!");
							 
						} else if (iret == 2 || iret == -1) { // error
							Log.error("iret : " + iret);
							isStop = true;
						}
						
						if(isStop) {
							Log.info("Stop Work Thread : script mig : " + this.Type );
							break;
						} 
						
						
						// main while�� ó��, mig_script �� ���� �������� ���ߴ� �κп� ���ؼ� ó��
						if("Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_SCRIPT_ERROR"))) {
							Log.info("Ignore Mig Script Error : Ongoing..");
							
						} else {
							// �߰��� ���� ���� ��� step �����ϰ� ���� ��� ó��
							Log.info("Ignore Mig Script Error : Stop..");
							break;
						}

					}
					
				} else if(iret == 2 ) { // index && idx >= 2 �̻� ó��
					Log.info("Index Processing : index count more than 2");
					//boolean isStop2 = false;
					
					// LOCK ������Ʈ ó�� �ʿ�
					if( !script.uptPreMigInfoMI(conn, this.Type)) { 
						// Log.error("uptMigLockInfo Fail");
						
						// main while�� ó��
						//break;
						
						// select ~ for update ���� update ���а� �߻��ϴ� ������ �߻��ϸ� �ȵǴ� ������ ����
						Log.error("uptPreMigInfoMI Fail, update count is 0");
						script.setErr(conn, this.Type, "ERR", "uptPreMigInfoMI Fail : update count is 0");
						
						continue;
					}
					
					// while�� ����� getScriptInfoMI() ���ϰ��� ���� �����ϵ��� ó��
					while( script.getScriptInfoMI(conn, this.Type) == 0 ) {
						
						/* 3. ��ũ��Ʈ �� �۾�, ��� LOCK ó�� */
						if( !script.uptPreMigInfo(conn, this.Type, Props.getProperty("ID")+"_"+Thread.currentThread().getName(), 1 )) { 
							// ��Ƽ�ε��� ��� status='LOCK' ó���� ��� ���ؼ� 'ING' ���� ���� ���а��̶� ����ó�� ���� ���� �����ʿ�
							Log.error("uptPreMigInfo Fail, Multi Index, update count is 0");
							script.setErr(conn, this.Type, "ERR", "uptPreMigInfo Fail : Multi Index, update count is 0");
							
							// ���۽��� �̵�
							continue;
						}
						
						/* �� �۾� ó�� */
						if(mig_script(script, conn)) {
							Log.info("Unit Script Migration Complete : index count is more than 2");
						} else { 
							/* ��ó�� �ϴٰ� ��������, ����������� skip �ϵ��� ó��
							 * �α�ó���� mig_index() �ȿ��� ó���ϹǷ�, �߰� �α����� �ʾ���
							 * 
							 */
							Log.warn("Unit Script Migration Not Complete : index count is more than 2");
							// sub while �� ���η� ����
							// continue;
							
							 /* Connection ��ȿ�� üũ */
							iret = dbc.isValidConn(conn);
							if(iret == 1) { // reconnect
								tds.setURL(Props.getProperty("CONN_URL"));
								tds.setUser(Props.getProperty("RULE_SCHEMA"));
								tds.setPassword(Props.getProperty("RULE_PASS"));
								conn = tds.getConnection();
								conn.setAutoCommit(false);
								
								Log.info("Reconnect DB");
								script.setErr(conn, this.Type, "ERR", "Abnormal Session Close!");
								 
							} else if (iret == 2 || iret == -1) { // error
								Log.error("iret : " + iret);
								isStop = true;
							}
							
						} // mig_script end
						
						
						 // ����ó�� �Ϸ�
						 if(isStop) {
							 Log.info("Stop Work Thread : index count is more than 2");
							 break;
						 } 
						
						
					} // sub while end(index count > 1)
					
					
				} else { // ����ó��
					Log.error("Error in setMigScriptLock()");
					break;
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
	
	/* ���Ϸ��� ó��, �����϶� true ó�� */
	private boolean mig_script(ScriptMig script, Connection conn) {
		boolean isStop = false;
		
		/* 4. ��ũ��Ʈ ��� */
		String ddl = null;
		

		if(CONSTRAINT.equalsIgnoreCase(this.Type) && 
				(DDEnb.equalsIgnoreCase(script.getScriptPath()) || DDEnbNo.equalsIgnoreCase(script.getScriptPath()) ) 
				) { // Disabled�� Constraint�� Ȱ��ȭ ��Ű�� ��� ��� ��
			ddl = script.get_ddl(conn);
		} else if( INDEX.equalsIgnoreCase(this.Type) && (DDRebuild.equalsIgnoreCase(script.getScriptPath()) ) ) { 
			// UNUSABLE INDEX�� ������ ��� ��� ��
			ddl = script.get_ddl();
		} else { // ���Ͽ��� ddl �о���̴� ���
			ddl = script.get_ddl(this.Type, Props.getProperty("SCRIPT_HOME"));
		}
		
		if (ddl == null || "".equals(ddl) ) {
			// �������� �ʿ�
			
			if(CONSTRAINT.equalsIgnoreCase(this.Type) && 
					(DDEnb.equalsIgnoreCase(script.getScriptPath()) || DDEnbNo.equalsIgnoreCase(script.getScriptPath()) )
					) { // �������� ���� ���� �����Ƿ� CHECK �ϴ� �������� ó��
				script.setErr(conn, this.Type, "CHECK", "4. get_ddl fail, Not Exists disabled Constraints");
			} else {
				Log.error("Stop 4. get_ddl");
				script.setErr(conn, this.Type, "ERR", "4. get_ddl fail, File Not Found OR File Content is empty");
			}
			
			// �����̺� ������� ���� �����ʿ�!!!
			isStop = true;
		}
		
		/* 5. ��ũ��Ʈ ���� */
		long elapsed_ms = 0;
		if( !isStop ) {
			elapsed_ms = script.execute_ddl(conn, this.Type, ddl, Props.getProperty("RULE_SCHEMA"), Props.getProperty("SWITCH_OWNER_SCHEMA") );
			if( elapsed_ms == -1 ) {
				Log.error("Stop 5. execute_ddl");
				script.setErr(conn, this.Type, "ERR", "5. execute_ddl fail : "+script.getErrMsg() );
				// �����̺� ������� ���� �����ʿ�!!!
				isStop = true;
			}
		}


		/* 6. ��ũ��Ʈ �� �۾� */
		 if( !isStop && !script.uptPostMigInfo(conn, this.Type, elapsed_ms)) {
			 
			 Log.error("uptPostMigInfo Fail!");
			 
			 // �۾� �� update ���� ����, ������� �ʿ�
			 script.setErr(conn, this.Type, "ERR", "6. uptPostMigInfo Fail!");
			 isStop = true;
			 
		 }
		
		return isStop ? false : true;
		
	}
	
} // class end
	


