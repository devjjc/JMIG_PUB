package com.tmax.jjc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.logging.log4j.Logger;

import com.tmax.tibero.jdbc.ext.TbDataSource;

public class ThreadGroupStat implements Runnable {

	private Properties Props = null;
	private Logger Log = null;
	
	public ThreadGroupStat(Properties p, Logger l) {
		Props = p;
		Log = l;
	}
	
	@Override
	
	public void run() {
		
		
		TbDataSource tds = new TbDataSource();
		Connection conn = null;
		boolean isStop = false;
		
		DBCheck dbc = new DBCheck(Log);
		
		StatMig stat = new StatMig(Props.getProperty("STAT_RULE_NM"), Props.getProperty("MIG_TAB_RULE_NM"), Props.getProperty("MIG_SCRIPT_RULE_NM"), 
				Props.getProperty("STAT_CHK_DEPENDENCY"), Integer.parseInt(Props.getProperty("STAT_START_MIGSEQ")), Log);

		try {
			tds.setURL(Props.getProperty("CONN_URL"));
			tds.setUser(Props.getProperty("RULE_SCHEMA"));
			tds.setPassword(Props.getProperty("RULE_PASS"));
			conn = tds.getConnection();
			conn.setAutoCommit(false);

			long elapsed_ms = 0;
			
			/* 1. ������ �����ȸ */
			while(true) {
				int iret = stat.getMigTabInfo(conn);
				
				switch(iret) {
				case 1: // ���� ������
					Log.info("Not Exists Gathering Stat Table");
					isStop = true;
					break;
				case 2: // �̰���� ����, ����ȸ �ʿ�
					Log.trace("repeat getMigTabInfo");
					if(stat.getMigTabInfo(conn) != 0) {
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
			
				
				/* 2. ������ ��� lock */

				if (!stat.setMigTabLock(conn)) {
					Log.debug("setMigTabLock Fail, Repeat Step[1]");
					
					// select ~ for update ����, ���ü��� ���Ͽ� ���ɼ��� �����Ƿ� 1���̵� �ʿ�
					continue;

				}
				

				/* 3. ������ �� �۾� */
				if( !stat.uptPreMigInfo(conn, Props.getProperty("ID")+"_"+Thread.currentThread().getName() )) { 
					// select ~ for update ���� update ���а� �߻��ϴ� ������ �߻��ϸ� �ȵǴ� ������ ����
					Log.error("uptPreMigInfo Fail, update count is 0");
					stat.setErr(conn, "ERR", "uptPreMigInfo Fail : update count is 0");
					
					// ���� �߻� �� �ٽ� �������� �̵�
					continue;
				
				}
				
				/* 4. ������ */
				elapsed_ms = stat.gather_stat(conn);
				if( elapsed_ms  == -1 ) {
					Log.error("Stop 4. gather_stat");
					stat.setErr(conn, "ERR", "gather_stat fail : "+stat.getErrMsg() );
					
					 /* Connection ��ȿ�� üũ */
					 iret = dbc.isValidConn(conn);
					 
					 if(iret == 1) { // reconnect
						tds.setURL(Props.getProperty("CONN_URL"));
						tds.setUser(Props.getProperty("RULE_SCHEMA"));
						tds.setPassword(Props.getProperty("RULE_PASS"));
						conn = tds.getConnection();
						conn.setAutoCommit(false);
						
						Log.info("Reconnect DB");
						stat.setErr(conn, "ERR", "Abnormal Session Close!");
					
					} else if (iret == 2 || iret == -1) { // error
						Log.error("iret : " + iret);
						isStop = true;
					} 
					
					// ���� �߻� �� �ٽ� �������� �̵�
					continue;
				}
				
				 /* 5. ������ �� �۾� */
				 if( !stat.uptPostMigInfo(conn, elapsed_ms )) {
					 
					 Log.error("Stop 5. uptPostMigInfo Fail!");
					 
					 // �۾� �� update ���� ����, ������� �ʿ�
					 stat.setErr(conn, "ERR", "uptPostMigInfo Fail : update count is 0");
					 
					// ���� �߻� �� �ٽ� �������� �̵�
					continue;
				 }
				 
				 // ����ó�� �Ϸ�
				 Log.info("Unit Gather Stat Complete!");
				 
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
	



