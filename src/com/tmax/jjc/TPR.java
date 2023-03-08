package com.tmax.jjc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.Logger;

import com.tmax.tibero.TbTypes;
import com.tmax.tibero.jdbc.driver.TbResultSetMetaData;
import com.tmax.tibero.jdbc.ext.TbDataSource;

public class TPR {
	
	private TbDataSource tds = null;
	private Connection conn = null;
	private Logger logger = null;
	private String snapshot_all = null;
	
	TPR(String isSnapshotAll, Logger l) {
		snapshot_all = isSnapshotAll;
		logger = l;		
	}
	
	public void initConn(String url, String user, String pass) {
		try {
			tds = new TbDataSource();
			tds.setURL(url);
			tds.setUser(user);
			tds.setPassword(pass);
			this.conn = tds.getConnection();
			this.conn.setAutoCommit(false);
			logger.info("Success to TPR Connection Creation");
			
		} catch (SQLException e) {
			logger.error("Fail to TPR Connection Init ");
			logger.error(e.getMessage());
		}
	}
	
	public void closeConn() {
		try {
			if (null != this.conn) { this.conn.close(); }
			logger.info("Success to TPR Connection Close");
		} catch (SQLException e) {
			logger.error("Fail to TPR Connection Close ");
			logger.error(e.getMessage());
		}
		
	}
	
	public void printRS(ResultSet rs) throws SQLException {
		TbResultSetMetaData rsmd=  (TbResultSetMetaData)rs.getMetaData();
		// StringBuilder format = new StringBuilder("");
		StringBuilder params = new StringBuilder("");
		
		logger.info("--------------------------------------------------------------------------------------------");
		for (int i=1; i <= rsmd.getColumnCount(); i++) {
			// logger.info("%s\t", rsmd.getColumnName(i));
			// logger.printf(Level.INFO, "%s\t", rsmd.getColumnName(i));
			
			if( i == 1 ) {
				params.append(rsmd.getColumnName(i));
			} else if( i == 4 || i==5 ) {
				params.append("\t\t").append(rsmd.getColumnName(i));
			} else {
				params.append("\t").append(rsmd.getColumnName(i));
			}
			
		} // for
		// logger.info("format : " + format.toString());
		// logger.info("params : " + params.toString());
		
		logger.info(params.toString());
		
		logger.info("--------------------------------------------------------------------------------------------");

		 while(rs.next()) {
		//rs.getInt("empno"); //어떤 테이블을 선택하냐에 따라 다르니까 get int/double/String 정할수가 없어 
			 
			// 초기화
			params.setLength(0);
				
			for (int i = 1; i <=rsmd.getColumnCount(); i++) {
				//크게는 String, int, Date, double (자주쓰는애들)
				//System.out.println(rsmd.getColumnType(i)); //1, 12 등 숫자형을 돌림
				//System.out.println(rsmd.getColumnTypeName(i)); //NUMBER, VARCHAR2 등 문자 돌림
				int scale = rsmd.getScale(i); // Number(7,2) 뒤에 2가 scale임
				int columnType = rsmd.getColumnType(i);
	
				if (columnType == TbTypes.NUMERIC && scale ==0) { //정수란 소리
					// logger.info("%d\t",rs.getInt(i));
					// logger.printf(Level.INFO, "%d\t",rs.getInt(i));
					params.append(rs.getInt(i)).append("\t");
	
				}else if(columnType == TbTypes.NUMERIC && scale !=0) {//실수란 소리
					// logger.printf(Level.INFO, "%.2f\t", rs.getDouble(i));
					params.append(rs.getDouble(i)).append("\t");
	
				}else if(columnType == TbTypes.VARCHAR|| columnType == TbTypes.CLOB) {//문자
					// logger.printf(Level.INFO, "%s\t", rs.getString(i));
					params.append(rs.getString(i)).append("\t");
	
				}else if(columnType == TbTypes.DATE|| columnType == TbTypes.TIMESTAMP) {//날짜
					// logger.printf(Level.INFO, "%tF\t",  rs.getDate(i));
					params.append("\t").append(rs.getDate(i)).append("\t");
					//날짜는 %f , %tF, %s 상관 없음
	
				}//if
			
			}//for
			
			logger.info(params.toString());
		
			// logger.info("\n");
		} //while
		 
		 // logger.info("\n");
		 logger.info("--------------------------------------------------------------------------------------------");

	} // printRS
	

	
	
	public boolean createTPRSnapshot(String explain) {
		
		Statement stmt = null;
		CallableStatement cstmt = null;
		ResultSet rs = null;
		
		StringBuilder query1 = new StringBuilder("select /* COM001 */ snap_id, thread# \n");
		query1.append("		   , to_char(begin_interval_time, 'yyyy/mm/dd hh24:mi:ss') begin_time \n");
		query1.append("	       , to_char(end_interval_time, 'yyyy/mm/dd hh24:mi:ss') end_time \n");
		query1.append("	       , snap_gid \n");
		query1.append("from ( \n");
		query1.append("	select snap_id \n");
		query1.append("	       , thread#, begin_interval_time, end_interval_time, snap_gid \n");
		query1.append("	       , row_number() over(order by begin_interval_time desc) rn \n");
		query1.append("	from sys._tpr_snapshot \n");
		query1.append(") t where t.rn <= 10 \n");

		
		logger.trace("query1 : \n"+ query1.toString());
		

		
		boolean ret=true;
		
		if( this.conn != null ) {
			try {
				
				if("Y".equalsIgnoreCase(snapshot_all)) {
					logger.info("TPR(GLOBAL) : " + explain);
					cstmt = this.conn.prepareCall("call dbms_tpr.create_snapshot_all()");
				} else { // local
					logger.info("TPR(LOCAL) : " + explain);
					cstmt = this.conn.prepareCall("call dbms_tpr.create_snapshot()");
				}
				
				// stmt.execute("exec dbms_tpr.create_snapshot_all()");
				cstmt.executeQuery();
				
				// 스냅샷 생성이후에 조회이전에 잠깐 대기
				Thread.sleep(5000);
				
				stmt = this.conn.createStatement();
				rs = stmt.executeQuery(query1.toString());
				printRS(rs);
			
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				ret = false;
			} catch (SQLException e) {
				logger.error(e.getMessage());
				ret = false;
			} finally {
				try {
					if (null != cstmt) { cstmt.close(); }
					if (null != rs) { rs.close(); }
					if (null != stmt) { stmt.close(); }
				} catch (SQLException e) {
					logger.error(e.getMessage());
					ret = false;
				}
			}
		} else {
			ret = false;
		}

		
		return ret;
	}
}
