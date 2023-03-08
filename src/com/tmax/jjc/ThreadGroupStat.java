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
			
			/* 1. 통계수집 대상조회 */
			while(true) {
				int iret = stat.getMigTabInfo(conn);
				
				switch(iret) {
				case 1: // 수집 대상없음
					Log.info("Not Exists Gathering Stat Table");
					isStop = true;
					break;
				case 2: // 이관대상 존재, 재조회 필요
					Log.trace("repeat getMigTabInfo");
					if(stat.getMigTabInfo(conn) != 0) {
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
			
				
				/* 2. 통계수집 대상 lock */

				if (!stat.setMigTabLock(conn)) {
					Log.debug("setMigTabLock Fail, Repeat Step[1]");
					
					// select ~ for update 실패, 동시성에 의하여 가능성이 있으므로 1번이동 필요
					continue;

				}
				

				/* 3. 통계수집 전 작업 */
				if( !stat.uptPreMigInfo(conn, Props.getProperty("ID")+"_"+Thread.currentThread().getName() )) { 
					// select ~ for update 이후 update 실패가 발생하는 내용은 발생하면 안되는 에러로 보임
					Log.error("uptPreMigInfo Fail, update count is 0");
					stat.setErr(conn, "ERR", "uptPreMigInfo Fail : update count is 0");
					
					// 에러 발생 시 다시 시작지점 이동
					continue;
				
				}
				
				/* 4. 통계수집 */
				elapsed_ms = stat.gather_stat(conn);
				if( elapsed_ms  == -1 ) {
					Log.error("Stop 4. gather_stat");
					stat.setErr(conn, "ERR", "gather_stat fail : "+stat.getErrMsg() );
					
					 /* Connection 유효성 체크 */
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
					
					// 에러 발생 시 다시 시작지점 이동
					continue;
				}
				
				 /* 5. 통계수집 후 작업 */
				 if( !stat.uptPostMigInfo(conn, elapsed_ms )) {
					 
					 Log.error("Stop 5. uptPostMigInfo Fail!");
					 
					 // 작업 후 update 수행 실패, 에러기록 필요
					 stat.setErr(conn, "ERR", "uptPostMigInfo Fail : update count is 0");
					 
					// 에러 발생 시 다시 시작지점 이동
					continue;
				 }
				 
				 // 정상처리 완료
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
	



