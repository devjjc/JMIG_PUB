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
			
			// 내부변수
			int iret = 0;
			boolean isStop = false;
			
			/* 1. 스크립트 대상조회 */
			while(true) {
				// 변수값 초기화
				isStop = false;
				
				// connection 초기화 고려필요
				
				iret = script.getMigScriptInfo(conn, this.Type);
				switch(iret) {
				case 1: // 이관 대상없음
					Log.info("Not Exists Migration Script");
					isStop = true;
					break; 
				case 2: // 이관대상 존재, 재조회 필요
					Log.trace("repeat getMigScriptInfo");
					if(script.getMigScriptInfo(conn, this.Type) != 0) {
						Log.error("repeat Fail");
						isStop = true;
					} else {
						Log.debug("repeat select success");
					}
					break;
				case 100:  // 에러발생
					Log.error("getMigScriptInfo Error!!");
					isStop = true;
					break;
				}
				
				// 이후 단계 진행여부 체크
				if(isStop) {
					break;
				}
			
				/* 2. 스크립트 대상 lock */
				iret = script.setMigScriptLock(conn, this.Type);
				if ( iret == 0) {
					Log.debug("setMigScriptLock Fail, Repeat Step[1]");
					
					// select ~ for update 실패, 동시성에 의하여 가능성이 있으므로 1번이동 필요
					continue;

				} else if(iret == 1 ) { // index && idx_cnt =1 처리, table, constraint, etc 처리
					/* 3. 스크립트 전 작업 */
					if( !script.uptPreMigInfo(conn, this.Type, Props.getProperty("ID")+"_"+Thread.currentThread().getName(), 0 )) { 
						
						// select ~ for update 이후 update 실패가 발생하는 내용은 발생하면 안되는 에러로 보임
						Log.error("uptPreMigInfo Fail, update count is 0");
						script.setErr(conn, this.Type, "ERR", "uptPreMigInfo Fail : update count is 0");
						
						continue;
					}
					
					/* 후 작업 처리 */
					if(mig_script(script, conn)) {
						Log.info("Unit Script Migration Complete : " + this.Type);
					} else {
						
						Log.warn("Unit Script Migration Not Complete : " + this.Type);
						
						 /* Connection 유효성 체크 */
						// 이관중 종료될때만 효과가 있음, 매번 체크하지 않기 위함
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
						
						
						// main while문 처리, mig_script 시 에러 맞은이후 멈추는 부분에 대해서 처리
						if("Y".equalsIgnoreCase(Props.getProperty("IGNORE_MIG_SCRIPT_ERROR"))) {
							Log.info("Ignore Mig Script Error : Ongoing..");
							
						} else {
							// 중간에 에러 맞을 경우 step 중지하고 싶을 경우 처리
							Log.info("Ignore Mig Script Error : Stop..");
							break;
						}

					}
					
				} else if(iret == 2 ) { // index && idx >= 2 이상 처리
					Log.info("Index Processing : index count more than 2");
					//boolean isStop2 = false;
					
					// LOCK 업데이트 처리 필요
					if( !script.uptPreMigInfoMI(conn, this.Type)) { 
						// Log.error("uptMigLockInfo Fail");
						
						// main while문 처리
						//break;
						
						// select ~ for update 이후 update 실패가 발생하는 내용은 발생하면 안되는 에러로 보임
						Log.error("uptPreMigInfoMI Fail, update count is 0");
						script.setErr(conn, this.Type, "ERR", "uptPreMigInfoMI Fail : update count is 0");
						
						continue;
					}
					
					// while문 종료는 getScriptInfoMI() 리턴값에 따라서 종료하도록 처리
					while( script.getScriptInfoMI(conn, this.Type) == 0 ) {
						
						/* 3. 스크립트 전 작업, 대상 LOCK 처리 */
						if( !script.uptPreMigInfo(conn, this.Type, Props.getProperty("ID")+"_"+Thread.currentThread().getName(), 1 )) { 
							// 멀티인덱스 경우 status='LOCK' 처리된 대상에 대해서 'ING' 상태 변경 실패건이라 에러처리 이후 이후 진행필요
							Log.error("uptPreMigInfo Fail, Multi Index, update count is 0");
							script.setErr(conn, this.Type, "ERR", "uptPreMigInfo Fail : Multi Index, update count is 0");
							
							// 시작시점 이동
							continue;
						}
						
						/* 후 작업 처리 */
						if(mig_script(script, conn)) {
							Log.info("Unit Script Migration Complete : index count is more than 2");
						} else { 
							/* 후처리 하다가 에러나면, 다음대상으로 skip 하도록 처리
							 * 로그처리는 mig_index() 안에서 처리하므로, 추가 로깅하지 않았음
							 * 
							 */
							Log.warn("Unit Script Migration Not Complete : index count is more than 2");
							// sub while 문 서두로 복귀
							// continue;
							
							 /* Connection 유효성 체크 */
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
						
						
						 // 정상처리 완료
						 if(isStop) {
							 Log.info("Stop Work Thread : index count is more than 2");
							 break;
						 } 
						
						
					} // sub while end(index count > 1)
					
					
				} else { // 에러처리
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
	
	/* 동일로직 처리, 정상일때 true 처리 */
	private boolean mig_script(ScriptMig script, Connection conn) {
		boolean isStop = false;
		
		/* 4. 스크립트 얻기 */
		String ddl = null;
		

		if(CONSTRAINT.equalsIgnoreCase(this.Type) && 
				(DDEnb.equalsIgnoreCase(script.getScriptPath()) || DDEnbNo.equalsIgnoreCase(script.getScriptPath()) ) 
				) { // Disabled된 Constraint를 활성화 시키는 기능 사용 시
			ddl = script.get_ddl(conn);
		} else if( INDEX.equalsIgnoreCase(this.Type) && (DDRebuild.equalsIgnoreCase(script.getScriptPath()) ) ) { 
			// UNUSABLE INDEX를 리빌드 기능 사용 시
			ddl = script.get_ddl();
		} else { // 파일에서 ddl 읽어들이는 방식
			ddl = script.get_ddl(this.Type, Props.getProperty("SCRIPT_HOME"));
		}
		
		if (ddl == null || "".equals(ddl) ) {
			// 수행종료 필요
			
			if(CONSTRAINT.equalsIgnoreCase(this.Type) && 
					(DDEnb.equalsIgnoreCase(script.getScriptPath()) || DDEnbNo.equalsIgnoreCase(script.getScriptPath()) )
					) { // 존재하지 않을 수도 있으므로 CHECK 하는 수준으로 처리
				script.setErr(conn, this.Type, "CHECK", "4. get_ddl fail, Not Exists disabled Constraints");
			} else {
				Log.error("Stop 4. get_ddl");
				script.setErr(conn, this.Type, "ERR", "4. get_ddl fail, File Not Found OR File Content is empty");
			}
			
			// 룰테이블에 에러기록 이후 종료필요!!!
			isStop = true;
		}
		
		/* 5. 스크립트 수행 */
		long elapsed_ms = 0;
		if( !isStop ) {
			elapsed_ms = script.execute_ddl(conn, this.Type, ddl, Props.getProperty("RULE_SCHEMA"), Props.getProperty("SWITCH_OWNER_SCHEMA") );
			if( elapsed_ms == -1 ) {
				Log.error("Stop 5. execute_ddl");
				script.setErr(conn, this.Type, "ERR", "5. execute_ddl fail : "+script.getErrMsg() );
				// 룰테이블에 에러기록 이후 종료필요!!!
				isStop = true;
			}
		}


		/* 6. 스크립트 후 작업 */
		 if( !isStop && !script.uptPostMigInfo(conn, this.Type, elapsed_ms)) {
			 
			 Log.error("uptPostMigInfo Fail!");
			 
			 // 작업 후 update 수행 실패, 에러기록 필요
			 script.setErr(conn, this.Type, "ERR", "6. uptPostMigInfo Fail!");
			 isStop = true;
			 
		 }
		
		return isStop ? false : true;
		
	}
	
} // class end
	


