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
			
			/* 1. 마이그레이션 대상조회 */
			while(true) {
				int iret = tab.getMigTabInfo(conn);
				
				switch(iret) {
				case 1: // 이관 대상없음
					Log.info("Not Exists Migration Table");
					isStop = true;
					break;
				case 2: // 이관대상 존재, 재조회 필요
					Log.trace("repeat getMigTabInfo");
					if(tab.getMigTabInfo(conn) != 0) {
						Log.error("repeat Fail");
						isStop = true;
					} else {
						Log.debug("repeat select success");
					}
					break;
				case 100:  // 에러발생
					Log.error("getMigTabInfo Error!!");
					isStop = true;
					break;
				}
				
				// 이후 단계 진행여부 체크
				if(isStop) {
					break;
				}
			
				
				/* 2. 마이그레이션 대상 lock */

				if (!tab.setMigTabLock(conn)) {
					Log.debug("setMigTabLock Fail, Repeat Step[1]");
					
					// select ~ for update 실패, 동시성에 의하여 가능성이 있으므로 1번이동 필요
					continue;

				}
				

				/* 3. 이관수행 전 작업 */
				if( !tab.uptPreMigInfo(conn, Props.getProperty("ID")+"_"+Thread.currentThread().getName() )) { 
					
					// select ~ for update 이후 update 실패가 발생하는 내용은 발생하면 안되는 에러로 보임
					Log.error("uptPreMigInfo Fail, update count is 0");

					//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					tab.setErr(conn, "ERR", "uptPreMigInfo Fail : update count is 0");
					
					// 에러 발생 시 다시 시작지점 이동
					continue;
				
				}
				
				/* 4. migrator/dblink 수행 */
				String mig_type = tab.getMigType();
				TabMigRst mig_rst = null;
				
				if(mig_type != null && this.TOOL.equalsIgnoreCase(mig_type) ) { // table migrator
					
					if (!tab.migrator(Props.getProperty("MIGTOOL_NAME"), Props.getProperty("MIGTOOL_HOME"), Props.getProperty("MIGTOOL_LOGPATH"))) {
						// 수행종료 필요
						Log.error("migrator tool Fail!");
						//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
						tab.setErr(conn, "ERR", "4. migrator tool fail");
						
						// 룰테이블에 에러기록 이후 종료필요!!!
						isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					}
					
					// 이후 단계 진행여부 체크
					if( isStop ) {
						break;
					}
	
					/* 5. 로그파일 분석결과 저장 */
					mig_rst = tab.miglog_analyzer(Props.getProperty("MIGTOOL_LOGPATH"));
					 
					/* migrator가 수행되었다면, 로그 얻는 부분에서만 실패 가능성이 있어서, 
					 * 예외처리 skip 하였음, 6단계에서 인자값에 따른 형태로 상태 체크진행 
					 */
					
				} else if (mig_type != null && this.LINK.equalsIgnoreCase(mig_type) ) { // dblink
					
					mig_rst = tab.dblink(conn, Props.getProperty("EXEC_NOLOGGING"), Props.getProperty("USE_LONGTYPE_FOR_ROWCNT"));
					
				} else { // unknown type
					Log.error("unknown mig_type!");
					//tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					tab.setErr(conn, "ERR", "unknown mig_type!");
					isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					
					// 이후 단계 진행여부 체크
					if( isStop ) {
						break;
					}
					
				}
				
				 /* 6. 이관수행 후 작업 */
				 if( !tab.uptPostMigInfo(conn, mig_rst.getRowCnt(), mig_rst.getElapsedMs(), mig_rst.getVerify() )) {
					 
					 Log.error("uptPostMigInfo Fail!");
					 
					 // tab.isConValid(conn, Props.getProperty("CONN_URL"), Props.getProperty("RULE_SCHEMA"), Props.getProperty("RULE_PASS"));
					 
					 // 작업 후 update 수행 실패, 에러기록 필요
					 tab.setErr(conn, "ERR", "6. uptPostMigInfo Fail!");
					 
					 isStop = "Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_TAB_ERROR")) ? false : true;
					 
					 /* Connection 유효성 체크 */
					 // 이관중이거나, 후처리에서 종료될때만 효과가 있음, 매번 체크하지 않기 위함
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
				 
				 
				 // 정상처리 완료
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
	


