package com.tmax.jjc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import org.apache.logging.log4j.Logger;

public class TabMig {
	private String rule_tab_nm = null;
	private int start_mig_seq = 0;
	private int tabno = 0;
	private String owner = null;
	private String name = null;
	private String src_owner = null;
	private String src_name = null;
	private String mig_type = null;	
	private String migrator_props = null;
	private String migrator_add_cmd = null;
	private String link_name = null;
	private String link_mode = null;
	private String link_src_condition = null;
	private String link_tar_part_nm = null;
	private int link_degree = 0;
	
	private String logfile_name = null;
	
	private String errMsg = null;
	
	private Logger Log = null;
	
	TabMig(String rule_table_name, int mig_tab_start_migseq, Logger l) {
		rule_tab_nm = rule_table_name;
		start_mig_seq = mig_tab_start_migseq;
		Log = l;		
	}
	
	public String getMigType() {
		return this.mig_type;
	}
	
	public int getMigTabInfo(Connection conn) {
		StringBuilder query1 = new StringBuilder("select /* TAB001 */ a.tabno, b.owner, b.name, b.src_owner, b.src_name, b.mig_type, b.migrator_props, b.migrator_add_cmd, "
				                               + "b.link_name, b.link_mode, b.link_src_condition, b.link_tar_part_nm, b.link_degree ");
		query1.append("	from ( \n");
		query1.append("		select tabno from (  \n");
		query1.append("			select tabno  \n");
		query1.append("			from ").append(this.rule_tab_nm).append(" \n");
		query1.append(" 		where status = 'READY' \n");
		
		// MIGTAB_START_MIGSEQ 파라미터 사용 시 처리
		if( this.start_mig_seq > 0 ) {
			query1.append(" 		      and mig_seq >= ").append(this.start_mig_seq).append(" \n");
		}
		
		query1.append(" 		order by mig_seq, size_kb desc \n");
		query1.append("		) a \n");
		query1.append("		where rownum = 1 \n");
		query1.append("	) a, ").append(this.rule_tab_nm).append(" b \n");
		query1.append("where a.tabno=b.tabno \n");
		
		
		//StringBuilder query2 = new StringBuilder("select /* TAB002 */ count(*) cnt \n");
		//query2.append("	from ").append(this.rule_tab_nm).append(" \n");
		//query2.append("where status = 'READY'");

		/* 대용량 고려 처리 */
		StringBuilder query2 = new StringBuilder("select /* TAB002 */ count(*) cnt \n");
		query2.append("	from ( select 1 from ").append(this.rule_tab_nm).append(" \n");
		
		// MIGTAB_START_MIGSEQ 파라미터 사용 시 처리
		if( this.start_mig_seq > 0 ) {
			//query1.append(" and mig_seq >= ").append(this.start_mig_seq).append(" \n");
			query2.append("        where status = 'READY' and mig_seq >= ").append(this.start_mig_seq).append(" and rownum < 2 )");
		} else {
			query2.append("        where status = 'READY' and rownum < 2 )");
		}
		
		
		
		Log.trace("query1 : \n"+ query1.toString());
		//Log.info("query1 : \n"+ query1.toString());
		Log.trace("query2 : \n"+ query2.toString());
		
		Statement stmt = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		
		//boolean bRet = false;
		// 0:정상조회, 1:대상없음, 2:대상있음(재조회필요), 100:에러발생  
		int iRet=0;
		
		
		try {
			stmt = conn.createStatement();
			rs1 = stmt.executeQuery(query1.toString());
			
			if( rs1.next() ) {
				this.tabno = rs1.getInt("TABNO");
				this.owner = rs1.getString("OWNER");
				this.name = rs1.getString("NAME");
				this.src_owner = rs1.getString("SRC_OWNER");
				this.src_name = rs1.getString("SRC_NAME");		
				this.mig_type = rs1.getString("MIG_TYPE");
				
				/* info for migrator */
				this.migrator_props = rs1.getString("MIGRATOR_PROPS");
				this.migrator_add_cmd = rs1.getString("MIGRATOR_ADD_CMD");
				
				/* info for dblink */
				this.link_name = rs1.getString("LINK_NAME");
				this.link_mode = rs1.getString("LINK_MODE");
				this.link_src_condition = rs1.getString("LINK_SRC_CONDITION");
				this.link_tar_part_nm = rs1.getString("LINK_TAR_PART_NM");
				this.link_degree = rs1.getInt("LINK_DEGREE");
				
				
				Log.trace("getMigTabInfo : [" + this.tabno + "] " + this.owner + "." + this.name);
				//Log.info("link_mode : " + this.link_mode);
				
			} else { // 이관대상 테이블이 없는지 체크
				rs2 = stmt.executeQuery(query2.toString());
				
				// 0 -> 이관대상 없음, 다른값 -> 이관대상 존재
				if(rs2.next()) {
					iRet = (rs2.getInt(1) == 0) ? 1 : 2 ;
				} else { // count(*) 쿼리라서 무조건 결과가 나와야 함, 없을경우 예외처리
					Log.error("Internal Error : nothing query2 result");
					iRet = 100;
				}
				
			} // if end 
			
		} catch (SQLException e) {
			Log.error(e.getMessage());
			iRet = 100;
		} finally {
			try {
				if (null != rs1) { rs1.close(); }
				if (null != rs2) { rs2.close(); }
				if (null != stmt) { stmt.close(); }
			} catch (SQLException e) {
				Log.error(e.getMessage());
			}
		}
		
		return iRet;
	} // getMigTabInfo
	
	
	public boolean setMigTabLock(Connection conn) {
		StringBuilder query = new StringBuilder("select /* TAB003 */ tabno \n");
		query.append("from ").append(this.rule_tab_nm).append(" \n");
		query.append("where tabno = ? and status = 'READY' for update");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean ret = false;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setInt(1, this.tabno);			
			rs = pstmt.executeQuery();

			if( rs.next() ) {
				ret = true;
			} 
			
		} catch (Exception e) {
			// e.printStackTrace();
			Log.error("error in setMigTabLock1");
			Log.error(e.getMessage());
		} finally {
			try {
				if (null != rs) { rs.close(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in setMigTabLock2");
				Log.error(e.getMessage());
			}
		}

		return ret ;
	} // setMigTabLock
	
	public boolean uptPreMigInfo(Connection conn, String id) {
		boolean isErr = false;
		StringBuilder query = new StringBuilder("update /* TAB004 */ ");
		query.append(this.rule_tab_nm).append(" set exec_node=?, status='ING', stime=sysdate \n");
		query.append("where tabno = ? and status = 'READY'");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, id);
			pstmt.setInt(2, this.tabno);
			
			count = pstmt.executeUpdate();
			//conn.commit();			
			
		} catch (Exception e) {
			isErr = true;
			Log.error("error in uptPreMigInfo1");
			Log.error(e.getMessage());
			//e.printStackTrace();
			count = 0;
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in uptPreMigInfo2");
				Log.error(e.getMessage());
				count = 0;
				// e.printStackTrace();
			}
		}

		return count>0 ? true:false ;
	}
	
	public boolean uptPostMigInfo(Connection conn, long row_cnt, long elapsed_ms, String verify) {
		boolean isErr = false;
		String v_status = "";
		String v_err_msg = "";
		
		StringBuilder query = new StringBuilder("update /* TAB005 */ ");
		query.append(this.rule_tab_nm);
		
		// 로그파싱 결과에 따른 query 분기진행
		if(verify==null) {
			// query.append(" set status='CHECK', etime=sysdate, row_cnt=?, elapsed_ms=?, verify=?, err_msg='check 5. miglog_analyzer step' \n");
			
			// 아래 바인드번호 일관성을 위해서 인자값 변경처리
			verify = "[NOT OK]";
			v_status = "CHECK";
			v_err_msg = "verify is null, check the log";
			//v_err_msg = "";
		} else if( "[OK]".equals(verify) ) { // NORMAL
			v_status = "END";
		} else if( "[ERROR]".equals(verify) ) { // ERROR
			v_status = "ERR";
			row_cnt = 0;
			elapsed_ms = 0;
			v_err_msg = this.errMsg.length()<=255 ? this.errMsg:this.errMsg.substring(0, 255);
		} else { 
			v_status = "CHECK";
			v_err_msg = "check to log";
		}
		
		query.append(" set status=?, etime=sysdate, row_cnt=?, elapsed_ms=?, verify=?, err_msg=?  \n");
		query.append("where tabno = ?");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			//pstmt.setLong(1, row_cnt);
			pstmt.setString(1, v_status);
			pstmt.setLong(2, row_cnt);
			pstmt.setLong(3, elapsed_ms);
			// 윗 query 생성문에서 null 처리를 해서 따로 처리하지 않음 -> NOT OK 부분
			pstmt.setString(4, verify.length()<=20 ? verify:verify.substring(0, 20));
			pstmt.setString(5, v_err_msg);
			pstmt.setInt(6, this.tabno);			
			count = pstmt.executeUpdate();
			
			// conn.commit();			
			
		} catch (Exception e) {
			isErr = true;
			Log.error("error in uptPostMigInfo1");
			Log.error(e.getMessage());
			count = 0;
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in uptPostMigInfo2");
				Log.error(e.getMessage());
				count = 0;
			}
		}

		return count>0 ? true : false ;
	}
	
	public boolean migrator(String migtool_name, String migtool_home, String migtool_logpath) {
		StringBuilder mig_cmd = new StringBuilder(migtool_name);
		mig_cmd.append(" PROPERTY_FILE=").append(this.migrator_props);
		mig_cmd.append(" SOURCE_SCHEMA=").append(this.src_owner);
		mig_cmd.append(" SOURCE_TABLE=").append(this.src_name);
		mig_cmd.append(" TARGET_SCHEMA=").append(this.owner);
		mig_cmd.append(" TARGET_TABLE=").append(this.name);
		
		// migrator 추가명령어 존재 시 추가
		if(this.migrator_add_cmd !=null) {
			mig_cmd.append(" ").append(this.migrator_add_cmd);
		}
		
		mig_cmd.append(" 1>").append(migtool_logpath).append("/");
		
		// 로그파일명 처리
		SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd_HHmmss");
		this.logfile_name = this.tabno + "_" + this.owner + "_" + this.name + "_" + fmt.format(System.currentTimeMillis()) + ".log";
				
		mig_cmd.append(this.logfile_name).append(" 2>&1 ");
		
		// mig_cmd.append("/").append(this.tar_owner).append("_").append(tar_name);
		// mig_cmd.append("/").append(this.tar_owner).append("_").append(tar_name).append(".log 2>&1 ");

		
		Log.trace("migrator target : [" + this.tabno + "] " + owner + "." + name);
		
		
		String[] cmd = new String[3];
		cmd[0] = "sh";
		cmd[1] = "-c";
		cmd[2] = mig_cmd.toString();
        
		Log.info("[MIGRATOR Command] : \n"+ cmd[2]);
        
        //Runtime rt = Runtime.getRuntime();
        Process ps = null;
        ProcessBuilder runBuilder = null;
        InputStream is = null;
        BufferedReader reader = null;
        String line;
        
        
        // exec migrator
        try {
			//ps = rt.exec(cmd);
        	runBuilder = new ProcessBuilder(cmd);
        	
        	// change directory
        	runBuilder.directory(new File(migtool_home));
        	
        	// execute
        	ps = runBuilder.start();
        	
            is = ps.getInputStream();
            reader = new BufferedReader(new InputStreamReader(is));
                        
            while ((line = reader.readLine()) != null) {
            	Log.trace(line);
            }
            
            reader.close();
            is.close();
            
            try {
            	int exitVal = ps.waitFor();
                if (exitVal == 0) ps.destroy(); 
            }
            catch (InterruptedException e) {
            	//e.printStackTrace();
            	Log.error("error in waiting migrator process");
            	Log.error(e.getMessage());
            	return false;
            }
		} catch (IOException e) {
			//e.printStackTrace();
			Log.error("error in running migrator");
			Log.error(e.getMessage());
			return false;
		}            

		return true;
	}
 
	public TabMigRst dblink(Connection conn, String exec_nologging, String use_longtype) {
		boolean isErr = false;
		Statement stmt = null;
		// HashMap<String, String> miginfo = new HashMap<String, String>();
		
		// 결과값 저장
		String verify = "[OK]";
		long elapsed_ms = 0;
		long row_cnt = 0;
		// miginfo.put("verify", "[OK]");
		
		// 에러메시지 초기화
		this.errMsg = "";
		
		// 수행시간 측정
		long startTime = System.currentTimeMillis();
		
		
		try {
			
			stmt = conn.createStatement();
			
			
			// current schema 변경처리
			if("Y".equalsIgnoreCase(exec_nologging)) {
				Log.debug("exec nologging : Y");
				
				stmt.addBatch("alter table "+this.owner+"."+this.name+" nologging");
			}
			
			// parallel enable dml
			
			stmt.addBatch("alter session enable parallel dml");

			
			StringBuilder query = null;
			

			// link 모드 : dpl/cpl 처리
			String sLinkMode = null;
			/*
			 * // if(this.link_mode != null ) { // Log.info("LINK_MODE : [" +
			 * this.link_mode+"]"); // } else { // Log.info("LINK_MODE : [NULL]"); // }
			 */				
			
			if( "CPL".equalsIgnoreCase(this.link_mode) || "LOCAL_CPL".equalsIgnoreCase(this.link_mode) ) {
				sLinkMode = "CPL";
			} else {
				sLinkMode = "APPEND";
			}
			
			// 256 이하까지 우선 제한
			if( this.link_degree >= 2 && this.link_degree <= 256 ) {
				//query = new StringBuilder("insert /*+ APPEND parallel(B ");
				query = new StringBuilder("insert /*+ ");
				query.append(sLinkMode);
				query.append(" parallel(B ").append(this.link_degree).append(" ) */ into ");
			} else {
				//query = new StringBuilder("insert /*+ APPEND */ into ");			
				query = new StringBuilder("insert /*+ ");
				query.append(sLinkMode);
				query.append(" */ into ");
			}

			query.append(this.owner).append(".").append(this.name);
			
			// insert partition 존재 시
			if(this.link_tar_part_nm !=null && !"".equals(this.link_tar_part_nm.trim() )) {
				query.append(" partition(").append(this.link_tar_part_nm).append(") B \n");
			} else {
				query.append(" B \n");
			}
			
			if( this.link_degree >= 2 && this.link_degree <= 256 ) {
				query.append("select /*+ parallel(A  ");
				query.append(this.link_degree).append(") */ * from ");
				
			} else {
				query.append("select * from ");			
			}
			
			// link 모드 : local 처리
			if( "LOCAL_DPL".equalsIgnoreCase(this.link_mode) || "LOCAL_CPL".equalsIgnoreCase(this.link_mode) ) {
				query.append(this.src_owner).append(".").append(this.src_name).append(" A \n");
			} else {
				query.append(this.src_owner).append(".").append(this.src_name).append("@").append(this.link_name).append(" A \n");
			}
			
			
			// select 조건 존재 시, 파티션 지정용도로 활용가능
			if(this.link_src_condition !=null && !"".equals(this.link_src_condition.trim() )) {
				query.append("where ").append(this.link_src_condition);
			} 
			
			// dblink 구문
			Log.info("[DBLINK SQL] : \n"+ query.toString());


			stmt.executeBatch();
			
			/* v1.6, Tibero JDBC 1.8 및 패치 미포함시 USE_LONGTYPE_FOR_ROWCNT=N 설정필요 */
			if("Y".equalsIgnoreCase(use_longtype)) {
				Log.debug("use_longtype : Y");
				row_cnt = stmt.executeLargeUpdate(query.toString());
			} else { 
				Log.debug("use_longtype : N");
				row_cnt = stmt.executeUpdate(query.toString());
			}
			
			//conn.commit();
			
//			if("Y".equalsIgnoreCase(switch_nologging)) {
//				Log.debug("switch nologging : Y");
//				stmt.addBatch("alter table "+this.owner+"."+this.name+" logging");
//			}
			
			
		} catch (SQLException e) {
			isErr = true;
			Log.error("execute dblink fail ");
			Log.error(e.getMessage());
			this.errMsg = e.getMessage();
			// ret = false;
			// miginfo.put("verify","[ERROR]");
			verify = "[ERROR]";
		} finally {
			try {
				// 후처리 단계에서 에러 맞을 경우 복원이 되지 않으므로 여기서 처리 정보 복구 처리
				/* 원래정보로 복원 처리 */
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				
				// conn 유효성 체크 및 필요시 재연결 맺도록 처리필요
				
				stmt.clearBatch();
				
				/* v1.5.1 */
				// degree 처리, 위 로직에서 기본으로 활성화 시켰으므로, 여기서도 기본으로 비활성화 처리
				stmt.addBatch("alter session disable parallel dml");

				
				if("Y".equalsIgnoreCase(exec_nologging)) {
					stmt.addBatch("alter table "+this.owner+"."+this.name+" logging");
				}
				
				stmt.executeBatch();
			
				if (null != stmt) { stmt.close(); }
				
			} catch (Exception e) {
				Log.error("error in post dblink processing");
				Log.error(e.getMessage());
				this.errMsg = e.getMessage();
				// miginfo.put("verify","[ERROR]");
				verify = "[ERROR]";
				// ret = false;
			} 
		}
		
		// long endTime = System.currentTimeMillis();
		// miginfo.put("elapsed_ms", (endTime-startTime)+"");
		elapsed_ms = System.currentTimeMillis()-startTime;
		
		//return ret ? endTime-startTime:-1;

		return new TabMigRst(verify, elapsed_ms, row_cnt);
	}

	public TabMigRst miglog_analyzer(String migtool_logpath) {
		StringBuilder logfile = new StringBuilder(migtool_logpath);
		logfile.append("/").append(this.logfile_name);
		
		// 에러메시지 초기화
		this.errMsg = "";
		
		// 결과값 저장
		String verify = null;
		long elapsed_ms = 0;
		long row_cnt = 0;
		//int result =0;
		
		//HashMap<String, String> miginfo = new HashMap<String, String>();
		//miginfo.put("result", "0"); // result key에는 결과값 갯수를 저장, 0일 경우 저장실패로 간주
		
		String line;
		
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(logfile.toString()));
			while((line = br.readLine()) != null) {
				// System.out.println(line);
				//if(line.contains("Elapsed Time (milliseconds)") && verify == null ) { // 수행시간 가져오기
				if(line.contains("Elapsed Time (milliseconds)") ) { // 수행시간 가져오기, 아래 처리 될 경우 수행시간이 집계되지 않아 verify == null 체크부분 skip
					 Log.trace(line);

					 // 로그패턴이 추후 변경 가능 시 파라미터화 고려 필요
					 String elapsed[] = line.split(" : ");
					 if(elapsed.length > 0 ) {
						 Log.trace("get elapsed_ms success: " + elapsed[1]);
						 // miginfo.put("elapsed_ms", elapsed[1]);
						 elapsed_ms = Long.parseLong(elapsed[1]);
						 //miginfo.put("result", "1"); //
					 }
					 
				} else if (line.contains("VERIFICATION") && verify == null ) { // row, verification 결과
					// verify == null 처리 경우 SQLException 발생이후 해당 로직을 skip 처리하기 위함
					
					 Log.trace(line);
   					 // 숫자만 가져오기( ex>[VERIFICATION] Source Info : SCOTT.DEPT : 4 Rows, Target Info : TIBERO1.DEPT : 4 Rows [OK] )
					 // 대소문자 그대로 적용되므로, 아래형태 처럼 사용해도 무방할 듯
					 String ver1[] = line.split("Target Info : "+this.owner+"."+this.name+ " : ");
					 for(int i=0; i<ver1.length; i++) {
						 Log.trace("verify["+i+"] " + ver1[i]);
					 }
					 
					 if(ver1.length > 0 ) { // 4 Rows [OK]
						 Log.trace("VERIFICATION : " + ver1[1]);
						 
						 // 로그패턴이 추후 변경 가능 시 파라미터화 고려 필요
						 // 공백 중요, ParseLong 시 뒤에 스페이스 있으면 변환에러 발생
						 String ver2[] = ver1[1].split(" Rows ");
						 
//						 for(int i=0; i<verify2.length; i++) {
//							 System.out.println("verify2["+i+"] " + verify2[i]);
//						 }
						 
						 if(ver2.length > 0 ) {
							 Log.trace("get rows, verify success!");
							 //miginfo.put("row_cnt", ver2[0]);
							 //miginfo.put("verify", ver2[1]);
							 row_cnt = Long.parseLong(ver2[0]);
							 verify = ver2[1];
							 //miginfo.put("result", "2"); 
						 }

					 } // if - verify.length
					
				// Exception 중복 발생할 경우 최초 에러를 출력하기 위함
			    // 20221110, SQLException -> Exception 로그변경 처리
				//} else if (line.contains("java.sql.SQLException") && !"[ERROR]".equals(verify) ) {
				} else if (!"[ERROR]".equals(verify) && line.contains("Exception") && line.contains("java.") ) {
					Log.trace(line);
					verify = "[ERROR]";
					this.errMsg = line;
				}
				
			} // while end
		} catch (FileNotFoundException e) {
			Log.error("LogFile Not Found : "+logfile.toString());
			Log.error(e.getMessage());
		} catch (Exception e) {
			Log.error("miglog_analyzer error");
			Log.error(e.getMessage());
		}	
	
		return new TabMigRst(verify, elapsed_ms, row_cnt);
	}

	
	public void setErr(Connection conn, String status, String err_msg) {
		boolean isErr = false;
		
		StringBuilder query = new StringBuilder("update /* TAB006 */ ");
		query.append(this.rule_tab_nm).append(" set status=?, err_msg=?, etime=sysdate \n");
		query.append("where tabno = ?");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;

		try {
			// connection 유효성 검사
			if(conn.isValid(3)) {
				;
			}
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, status);
			pstmt.setString(2, err_msg.length()<=255 ? err_msg:err_msg.substring(0, 255));
			pstmt.setInt(3, this.tabno);
			
			pstmt.executeUpdate();
			//conn.commit();			
			
		} catch (Exception e) {
			isErr = true;
			Log.error("error in seterr1");
			Log.error(e.getMessage());
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in seterr2");
				Log.error(e.getMessage());
			}
		}

	} // end setErr
	
	
} // TabMig
