package com.tmax.jjc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.logging.log4j.Logger;

public class StatMig {
	final String INDEX = "INDEX";
	final String CONSTRAINT = "CONSTRAINT";
	
	private String rule_tab_nm = null;
	private String rule_tab_mig_nm = null;
	private String rule_tab_script_nm = null;
	private int start_mig_seq = 0;	
	private String statChkDep = null;
	
	private int tabno = 0;
	private String owner = null;
	private String name = null;
	
	private int estimate_percent = 0;
	private int degree = 0;
	private String method_opt = null;
	private String add_opt = null;
	private String errMsg = null;
	
	private Logger Log = null;
	
	StatMig(String rule_table_name, String rule_table_mig_name, String rule_table_script_name, String stat_chk_dep, int stat_start_migseq, Logger l) {
		rule_tab_nm = rule_table_name;
		rule_tab_mig_nm = rule_table_mig_name;
		rule_tab_script_nm = rule_table_script_name;
		statChkDep = stat_chk_dep;
		start_mig_seq = stat_start_migseq;
		Log = l;		
	}
	
	public String getErrMsg() {
		return this.errMsg;
	}
	
	public int getMigTabInfo(Connection conn) {
		StringBuilder query1 = new StringBuilder("select /* STAT001 */ a.tabno, b.owner, b.name, b.estimate_percent, b.degree, b.method_opt, b.add_opt \n");
		query1.append("from ( \n");
		query1.append("	       select tabno from (  \n");
		query1.append("	              select tabno  \n");
		query1.append("	              from ").append(this.rule_tab_nm).append(" a \n");
		query1.append(" where status = 'READY' \n");
		
		// ������ ������ ó��, ��Ƽ��� ��� �� �ʿ��� ���
		if( "Y".equalsIgnoreCase(this.statChkDep)) { 
			//Log.info("Check Dependency Constraint. ");
			
			// �ε���, �������� ������ üũ, ���� �� �Ϸᰡ �Ǿ� �־����
			query1.append(" and exists ( select 'x' from ( select status, count(*) over() tot_cnt \n");
			query1.append("                                from (select status \n");
			query1.append("                                      from ").append(this.rule_tab_script_nm).append(" b \n");
			query1.append("                                      where b.script_type IN ('").append(this.INDEX).append("', '").append(this.CONSTRAINT).append("') \n");
			query1.append("                                            and b.table_owner = a.owner and b.table_nm = a.name \n");
			query1.append("                                      union all select 'END' from dual \n");
			query1.append("                                      ) tt \n");
			query1.append("                              ) \n");
			query1.append("              where status = 'END' \n");
			query1.append("              group by tot_cnt \n");
			query1.append("              having count(*) = tot_cnt ) \n");
			
			// ���̺� ������ üũ
			query1.append(" and exists (select 'x' from ( select status, count(*) over() tot_cnt \n");
			query1.append("                               from ").append(this.rule_tab_mig_nm).append(" b where b.owner = a.owner and b.name = a.name ) tt \n");
			query1.append("           where tt.status = 'END' \n");
			query1.append("           group by tot_cnt \n");
			query1.append("           having count(*) = tot_cnt ) \n");	
		} 
		
		// START_MIGSEQ �Ķ���� ��� �� ó��, ��뷮�� ������ ���� �������� �κ��� ����
		if( this.start_mig_seq > 0 ) {
			query1.append(" and mig_seq >= ").append(this.start_mig_seq).append(" \n");
		}
		
		// �� ó��
		query1.append(" order by mig_seq, tabno \n");
		query1.append(" ) t \n");
		query1.append("where rownum = 1 ) a \n");
		query1.append("inner join ").append(this.rule_tab_nm).append(" b on a.tabno=b.tabno");
		
		/* ��뷮 ��� ó�� */
		StringBuilder query2 = new StringBuilder("select /* STAT002 */ count(*) cnt \n");
		query2.append("	    from ( \n");
		query2.append("	           select 1 cnt from ").append(this.rule_tab_nm).append(" \n");
		query2.append("            where status = 'READY' \n");
		
		// START_MIGSEQ �Ķ���� ��� �� ó��, INDEX�� ó��
		if( this.start_mig_seq > 0 ) { 
			query2.append("        and mig_seq >= ").append(this.start_mig_seq).append(" \n");
		}
		
		query2.append("        and rownum < 2 ) \n");		
		//query2.append("            where status = 'READY' and rownum < 2 ) \n");
		
		Log.trace("query1 : \n"+ query1.toString());
		Log.trace("query2 : \n"+ query2.toString());
		
		Statement stmt = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		
		//boolean bRet = false;
		// 0:������ȸ, 1:������, 2:�������(����ȸ�ʿ�), 100:�����߻�  
		int iRet=0;
		
		
		try {
			stmt = conn.createStatement();
			rs1 = stmt.executeQuery(query1.toString());
			
			if( rs1.next() ) {
				this.tabno = rs1.getInt("TABNO");
				this.owner = rs1.getString("OWNER");
				this.name = rs1.getString("NAME");
				this.estimate_percent = rs1.getInt("ESTIMATE_PERCENT");
				this.degree = rs1.getInt("DEGREE");
				this.method_opt = rs1.getString("METHOD_OPT");
				this.add_opt = rs1.getString("ADD_OPT");

				Log.trace("gatherStatInfo : [" + this.tabno + "] " + this.owner + "." + this.name);
				
			} else { // ������ ����� ������ üũ
				
				// ������ üũ ���� �� �������� üũ ��ŵ
				if("Y".equalsIgnoreCase(this.statChkDep)) {
					Log.info("Skip remaining object query because dependency check.");
					iRet = 1;
					
				} else {
					rs2 = stmt.executeQuery(query2.toString());
					
					// 0 -> �̰���� ����, �ٸ��� -> �̰���� ����
					if(rs2.next()) {
						iRet = (rs2.getInt(1) == 0) ? 1 : 2 ;
					} else { // count(*) ������ ������ ����� ���;� ��, ������� ����ó��
						Log.error("Internal Error : nothing query2 result");
						iRet = 100;
					}
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
		StringBuilder query = new StringBuilder("select /* STAT003 */ tabno \n");
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
		StringBuilder query = new StringBuilder("update /* STAT004 */ ");
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
			// conn.commit();			
			
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
	
	public boolean uptPostMigInfo(Connection conn, long elapsed_ms) {
		boolean isErr = false;
		StringBuilder query = new StringBuilder("update /* STAT005 */ ");
		query.append(this.rule_tab_nm);
		query.append(" set status='END', etime=sysdate, elapsed_ms=?  \n");
		query.append("where tabno = ?");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setLong(1, elapsed_ms);
			pstmt.setInt(2, this.tabno);
			
			count = pstmt.executeUpdate();
			// conn.commit();			
			
		} catch (Exception e) {
			isErr = true;
			
			Log.error("error in uptPostMigInfo1");
			Log.error(e.getMessage());
						
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in uptPostMigInfo2");
				Log.error(e.getMessage());
			}
		}

		return count>0 ? true : false ;
	}
	

	public long gather_stat(Connection conn) {
		
		CallableStatement cstmt = null;
		boolean ret = true;
		
		// �����޽��� �ʱ�ȭ
		this.errMsg = "";
		
		// ������ ��������
		StringBuilder stat_cmd = new StringBuilder("BEGIN dbms_stats.gather_table_stats(");
		stat_cmd.append(" ownname=>'").append(this.owner).append("'");
		stat_cmd.append(", tabname=>'").append(this.name).append("'");
		
		if( this.estimate_percent > 0 && this.degree <= 100 ) {
			stat_cmd.append(", estimate_percent=>").append(this.estimate_percent);
		}
		
		// 256 ���ϱ��� �켱 ����
		if( this.degree >= 2 && this.degree <= 256 ) {
			stat_cmd.append(", degree=>").append(this.degree);			
		}
		
		// 2�� �Ķ���ʹ� null�� ���ǹǷ� üũ����
		if(this.method_opt !=null) {
			stat_cmd.append(", method_opt=>'").append(this.method_opt).append("'");
		}
		
		// add_opt �Ķ���� ��� ��ü�������� ����� �Ǿ����
		if(this.add_opt !=null) {
			stat_cmd.append(", ").append(this.add_opt);
		}
		
		// ��ó��
		stat_cmd.append(" ); END;");
		
		Log.info("[Gather Stat Command] : "+ stat_cmd.toString() );
		
		// ����ð� ����
		long startTime = System.currentTimeMillis();
		
		
		try {
			cstmt = conn.prepareCall(stat_cmd.toString());
			cstmt.executeQuery();	
			
		} catch (SQLException e) {
			Log.error("[SQLException] gather_stat fail : "+ stat_cmd.toString());
			Log.error(e.getMessage());
			this.errMsg = e.getMessage();
			ret = false;
		} catch (Exception e) {
			Log.error("[Exception] gather_stat fail : "+ stat_cmd.toString());
			Log.error(e.getMessage());
			this.errMsg = e.getMessage();
			ret = false;
		} finally {
			try {
				if (null != cstmt) { cstmt.close(); }
			} catch (Exception e) {
				Log.error("error in gather_stat");
				Log.error(e.getMessage());
				this.errMsg = e.getMessage();
				ret = false;
			} 
		}
		
		long endTime = System.currentTimeMillis();
		
		return ret ? endTime-startTime:-1;
	}
	
	public void setErr(Connection conn, String status, String err_msg) {
		boolean isErr = false;
		
		StringBuilder query = new StringBuilder("update /* STAT006 */ ");
		query.append(this.rule_tab_nm).append(" set status=?, err_msg=?, etime=sysdate \n");
		query.append("where tabno = ?");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, status);
			pstmt.setString(2, err_msg.length()<=255 ? err_msg:err_msg.substring(0, 255));
			pstmt.setInt(3, this.tabno);
			
			pstmt.executeUpdate();
			// conn.commit();
			
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

	}
	
	
} // TabMig
