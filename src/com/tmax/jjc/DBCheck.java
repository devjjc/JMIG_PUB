package com.tmax.jjc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.Logger;

public class DBCheck {
	// 추후 파라미터화 필요
	private static int ReConnectCode=-90603;
	
	// 추후 파라미터화 필요
	private static String query ="select /* JMIG001 */ 1 from dual \n";
	private Logger logger = null;

	
	public DBCheck(Logger l) {
		logger = l;
	}
	
	
	/* 
	 * conn의 경우 세션킬을 맞을 경우 에러를 기록할 수 있는 세션이 없어서 기록불가 
	 * 10-14 05:28:19 [MIG-0003] ERROR - execute dblink fail 
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90405:I/O error while reading from the server. - End Of Stream
		10-14 05:28:19 [MIG-0003] ERROR - error in post dblink processing
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] ERROR - error in uptPostMigInfo1
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] ERROR - error in uptPostMigInfo2
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] ERROR - uptPostMigInfo Fail!
		10-14 05:28:19 [MIG-0003] ERROR - error in seterr1
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] ERROR - error in seterr2
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] INFO  - Unit Data Migration Complete!
		10-14 05:28:19 [MIG-0003] ERROR - JDBC-90603:Invalid operation: disconnected from the server.
		10-14 05:28:19 [MIG-0003] ERROR - getMigTabInfo Error!!
	 */
	
	
	public int isValidConn(Connection conn) {
		int ret = -1;
		
		
		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			if( rs.next() ) {
				ret = 0; // valid
			} 
			
		} catch (SQLException e) {
			// e.printStackTrace();
			logger.error("error in isValidConn1");
			logger.error(e.getMessage());
			
			logger.error("ErrorCode : " + e.getErrorCode());
			if( e.getErrorCode() == ReConnectCode ) {
				ret = 1;
			}
			
		} finally {
			try {
				if (null != rs) { rs.close(); }
				if (null != stmt) { stmt.close(); }
				if (ret == 1 ) {
					conn.close();
					logger.info("success to close connection.");
				}
				
			} catch (Exception e) {
				logger.error("error in isValidConn2");
				logger.error(e.getMessage());
				ret = 2;
			}
		}
		
		return ret;
	}

}
