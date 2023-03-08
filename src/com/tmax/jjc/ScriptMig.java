package com.tmax.jjc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.Logger;

public class ScriptMig {
	final String INDEX = "INDEX";
	final String CONSTRAINT = "CONSTRAINT";
	final String TABLE = "TABLE";
	final String ETC = "ETC";
	final String DDEnb = "DD_ENABLE";       // enable constraint
	final String DDEnbNo = "DD_ENABLE_NO"; // enable novalidate constraint
	final String DDRebuild = "DD_REBUILD"; // index rebuild
	final String Partition = "INDEX PARTITION"; // INDEX PARTITION
	final String SubPartition = "INDEX SUBPARTITION"; // INDEX SUBPARTITION

	private String rule_tab_nm = null;
	private String rule_tab_mig_nm = null;
	private int start_mig_seq = 0;
	private String idxChkDep = null;
	private String idxChkDepForPart = null;
	private String conChkDep = null;
	
	private int script_no = 0;
	private String owner = null;
	private String name = null;
	private String table_owner = null;
	private String table_nm = null;
	private String part_type = null;
	private String part_name = null;
	private String script_path = null;
	private int idx_cnt = 0;
	private int degree = 0;
	
	private Logger Log = null;
	
	private String errMsg = null;
	
	ScriptMig(String rule_table_name, String rule_table_mig_name, String idx_chk_dep, String idx_chk_dep_part, String con_chk_dep, int index_start_migseq, Logger l) {
		rule_tab_nm = rule_table_name;
		rule_tab_mig_nm = rule_table_mig_name;
		idxChkDep = idx_chk_dep;
		idxChkDepForPart = idx_chk_dep_part;
		conChkDep = con_chk_dep;
		start_mig_seq = index_start_migseq;
		Log = l;
		
	}
	
	public String getErrMsg() {
		return this.errMsg;
	}
	
	public String getScriptPath() {
		return this.script_path;
	}
	
	public int getMigScriptInfo(Connection conn, String script_type) {
		StringBuilder query1 = new StringBuilder("select /* SCRIPT001 */ a.script_no, b.owner, b.name, b.table_owner, b.table_nm, b.part_type, b.part_name, b.idx_cnt, b.degree, b.script_path from ( \n");
		query1.append("	select script_no from (  \n");
		query1.append("	       select script_no  \n");
		query1.append("	       from ").append(this.rule_tab_nm).append(" a \n");
		query1.append(" where script_type = ? and status = 'READY' \n");
		
		// index/constraint ��� ������ ó��, ��Ƽ��� ��� �� �ʿ��� ���
		if(this.INDEX.equalsIgnoreCase(script_type) && "Y".equalsIgnoreCase(this.idxChkDep)) { 
			//Log.info("Check Dependency Index. ");
			query1.append(" and exists (select 'x' from ( select status, count(*) over() tot_cnt \n");
			
			// ��Ƽ�Ǵ��� ������ üũ����, ��Ƽ�Ǵ����� �ƴѰ�쿡�� �������� ���ؼ� nvl ó�� �Ͽ��� 
			if( "Y".equalsIgnoreCase(this.idxChkDepForPart) ) {
				query1.append("                               from ").append(this.rule_tab_mig_nm).append(" b where b.owner = a.table_owner and b.name = a.table_nm and nvl(b.part_name, 1) = nvl(a.part_name, 1) ) tt \n");
			} else {
				query1.append("                               from ").append(this.rule_tab_mig_nm).append(" b where b.owner = a.table_owner and b.name = a.table_nm ) tt \n");
			}
			
			query1.append("           where tt.status = 'END' \n");
			query1.append("           group by tot_cnt \n");
			query1.append("           having count(*) = tot_cnt ) \n");
			
		} else if(this.CONSTRAINT.equalsIgnoreCase(script_type) && "Y".equalsIgnoreCase(this.conChkDep) ) {
			//Log.info("Check Dependency Constraint. ");
			// ���̺� ������ üũ, �ε����� ���� ��쵵 �����Ƿ� ������ üũ ����
			query1.append(" and exists (select 'x' from ( select status, count(*) over() tot_cnt \n");
			query1.append("                               from ").append(this.rule_tab_mig_nm).append(" b where b.owner = a.table_owner and b.name = a.table_nm ) tt \n");
			query1.append("           where tt.status = 'END' \n");
			query1.append("           group by tot_cnt \n");
			query1.append("           having count(*) = tot_cnt ) \n");	
			
			// �ε��� ������ üũ, ���� �� �Ϸᰡ �Ǿ� �־����
			query1.append(" and exists ( select 'x' from ( select status, count(*) over() tot_cnt \n");
			query1.append("                                from (select status \n");
			query1.append("                                      from ").append(this.rule_tab_nm).append(" b \n");
			query1.append("                                      where b.script_type = '").append(this.INDEX).append("' and b.table_owner = a.table_owner and b.table_nm = a.table_nm \n");
			query1.append("                                      union all select 'END' from dual \n");
			query1.append("                                      ) tt \n");
			query1.append("                              ) \n");
			query1.append("              where status = 'END' \n");
			query1.append("              group by tot_cnt \n");
			query1.append("              having count(*) = tot_cnt ) \n");
		}
		
		// START_MIGSEQ �Ķ���� ��� �� ó��, INDEX�� ó��
		if(this.INDEX.equalsIgnoreCase(script_type) && this.start_mig_seq > 0 ) { 
			query1.append(" and mig_seq >= ").append(this.start_mig_seq).append(" \n");
		}
		
		// �� ó��
		query1.append(" order by mig_seq, size_kb desc \n");
		query1.append(") t \n");
		query1.append("where rownum = 1 ) a \n");
		query1.append("inner join ").append(this.rule_tab_nm).append(" b on b.script_type = ? and a.script_no=b.script_no");
		
		/* ��뷮 ��� ó�� */
		StringBuilder query2 = new StringBuilder("select /* SCRIPT002 */ count(*) cnt \n");
		query2.append("	from ( \n");
		query2.append("	       select 1 cnt from ").append(this.rule_tab_nm).append(" \n");
		query2.append("        where script_type = ? and status = 'READY' \n");
		
		// START_MIGSEQ �Ķ���� ��� �� ó��, INDEX�� ó��
		if(this.INDEX.equalsIgnoreCase(script_type) && this.start_mig_seq > 0 ) { 
			query2.append("        and mig_seq >= ").append(this.start_mig_seq).append(" \n");
		}
		
		query2.append("        and rownum < 2 ) \n");
		
		Log.trace("query1 : \n"+ query1.toString());
		//Log.info("query1 : \n"+ query1.toString());
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		
		//boolean bRet = false;
		// 0:������ȸ, 1:������, 2:�������(����ȸ�ʿ�), 100:�����߻�  
		int iRet=0;
		
		
		try {
			Log.trace("getMigScriptInfo : execute query1");
			pstmt = conn.prepareStatement(query1.toString());
			pstmt.setString(1, script_type);
			pstmt.setString(2, script_type);
			rs1 = pstmt.executeQuery();
			
			if( rs1.next() ) {
				script_no = rs1.getInt("SCRIPT_NO");
				owner = rs1.getString("OWNER");
				name = rs1.getString("NAME");
				table_owner = rs1.getString("TABLE_OWNER");
				table_nm = rs1.getString("TABLE_NM");
				part_type = rs1.getString("PART_TYPE");
				part_name = rs1.getString("PART_NAME");
				idx_cnt = rs1.getInt("IDX_CNT");
				degree = rs1.getInt("DEGREE");
				script_path = rs1.getString("SCRIPT_PATH");
				
				Log.trace("getMigScriptInfo : " + owner + "." + name);
				
			} else { // �̰���� ���̺��� ������ üũ
				
				// ������ üũ ���� �� �������� üũ ��ŵ
				if( (this.INDEX.equalsIgnoreCase(script_type) && "Y".equalsIgnoreCase(this.idxChkDep))
						|| ( this.CONSTRAINT.equalsIgnoreCase(script_type) && "Y".equalsIgnoreCase(this.conChkDep) ) ) {
					Log.info("Skip remaining object query because dependency check.");
					iRet = 1;
					
				} else {
					Log.trace("getMigScriptInfo : execute query2");
					Log.trace("query2 : "+ query2.toString());
					
					
					pstmt2 = conn.prepareStatement(query2.toString());
					
					// query2 result�� ���;� �ϴµ�, ������ �ʾƼ� Ŭ����ó�� �Ͽ���, �ش� api ȣ������ ������
					pstmt2.clearParameters(); 
					pstmt2.setString(1, script_type);
					rs2 = pstmt2.executeQuery();
					
					// 0 -> �̰���� ����, �ٸ��� -> �̰���� ����
					if(rs2.next()) {
						Log.trace("processing query2 result");
						iRet = (rs2.getInt(1) == 0) ? 1 : 2 ;
					} else { // count(*) ������ ������ ����� ���;� ��, ������� ����ó�� -> clearParameters() ó������ ������, JDBC �������� üũ�ʿ�, �켱����ó��
						Log.error("Internal Error : nothing query2 result");
						iRet = 100;
					}
					
				} // if end - dependency
				

			} // if end -  rs1.next() 
		
			
		} catch (SQLException e) {
			Log.error(e.getMessage());
			iRet = 100;
		} finally {
			try {
				if (null != rs1) { rs1.close(); }
				if (null != rs2) { rs2.close(); }
				if (null != pstmt) { pstmt.close(); }
				if (null != pstmt2) { pstmt2.close(); }
			} catch (SQLException e) {
				Log.error(e.getMessage());
			}
		}
		
		return iRet;
	} // getMigScriptInfo

	public int getScriptInfoMI(Connection conn, String script_type) {
		StringBuilder query1 = new StringBuilder("select /* SCRIPT003 */ * from ( \n");
		query1.append("	select script_no, owner, name, part_type, part_name, degree, script_path, row_number() over(order by mig_seq, size_kb desc) rn \n");
		query1.append("	from ").append(this.rule_tab_nm).append(" \n");
		query1.append(" where script_type = ? and table_owner=? and table_nm = ? and status='LOCK' \n");
		query1.append(") t \n");
		query1.append("where t.rn = 1");
		
		Log.trace("query1 : \n"+ query1.toString());
		
		PreparedStatement pstmt = null;
		ResultSet rs1 = null;
		
		//boolean bRet = false;
		// 0:������ȸ, 1:������, 100:�����߻�  
		int iRet=0;
		
		
		try {
			Log.trace("getScriptInfoMI : execute query1");
			pstmt = conn.prepareStatement(query1.toString());
			pstmt.setString(1, script_type);
			pstmt.setString(2, this.table_owner);
			pstmt.setString(3, this.table_nm);
			rs1 = pstmt.executeQuery();
			
			if( rs1.next() ) {
				script_no = rs1.getInt("SCRIPT_NO");
				owner = rs1.getString("OWNER");
				name = rs1.getString("NAME");
				//table_owner = rs1.getString("TABLE_OWNER");
				//table_nm = rs1.getString("TABLE_NM");
				//idx_cnt = rs1.getInt("IDX_CNT");
				part_type = rs1.getString("PART_TYPE");
				part_name = rs1.getString("PART_NAME");
				degree = rs1.getInt("DEGREE");
				script_path = rs1.getString("SCRIPT_PATH");
				
				Log.trace("getScriptInfoMI : " + owner + "." + name);
				
			} else { // �������̺� �Ҽӵ� �ε��� ���� ������ ����
				Log.info("no more index in same table");
				iRet=1;
				
			} // if end 
		
			
		} catch (SQLException e) {
			Log.error(e.getMessage());
			iRet = 100;
		} finally {
			try {
				if (null != rs1) { rs1.close(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (SQLException e) {
				Log.error(e.getMessage());
			}
		}
		
		return iRet;
	} // getScriptInfoMI
	
	
	public int setMigScriptLock(Connection conn, String script_type) {
		StringBuilder query1 = new StringBuilder("select /* SCRIPT004 */ 1 \n");
		query1.append("from ").append(this.rule_tab_nm).append(" \n");
		query1.append("where script_type = ? and script_no = ? and status = 'READY' for update");
		//query1.append("where script_type = ? and owner=? and name = ? and status = 'READY' for update");
		
		// �ε����̸鼭 idx_cnt > 1 ��츸 ����Ǵ� SQL�̶� �б�ó�� ���� �ʾ���
		StringBuilder query2 = new StringBuilder("select /* SCRIPT005 */ 1 \n");
		query2.append("from ").append(this.rule_tab_nm).append(" \n");
		query2.append("where script_type = ? and table_owner=? and table_nm = ? and part_name is null and status = 'READY' for update");
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		//boolean ret = false;
		
		// ���ϰ� ó���� ���� ȣ���ϴºκп��� ����ó�� �����ʿ�
		int ret = 0;
		
		try {
			// �ε����̸鼭 idx_cnt 2�̻��� ��� ó��, �����۾���忡�� �������̺� �Ҽӵ� �ε��� ó���ϱ� ����
			if( this.INDEX.equals(script_type) && this.idx_cnt >= 2 ) { 
				Log.debug("exec query2 : \n"+ query2.toString());
				pstmt = conn.prepareStatement(query2.toString());
				pstmt.setString(1, script_type);
				pstmt.setString(2, this.table_owner);
				pstmt.setString(3, this.table_nm);
				rs = pstmt.executeQuery();

				if( rs.next() ) {
					// �ε��� 2���̻� ����ó��
					ret = 2; 
				} 
				
			} else { // �ε��� idx_cnt <=2, ���̺�, ��������, ��Ÿ ó�� 
				Log.debug("exec query1 : \n"+ query1.toString());
				pstmt = conn.prepareStatement(query1.toString());
				pstmt.setString(1, script_type);
				pstmt.setInt(2, this.script_no);
				//pstmt.setString(2, this.owner);
				//pstmt.setString(3, this.name);
				rs = pstmt.executeQuery();

				if( rs.next() ) {
					// �ε���(1��), ���̺�, �������� ó��
					ret = 1;
				} 
			}
			
			
		} catch (Exception e) {
			Log.error("error in setMigScriptLock1");
			Log.error(e.getMessage());
			ret = -1;
			// e.printStackTrace();
		} finally {
			try {
				if (null != rs) { rs.close(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in setMigScriptLock2");
				Log.error(e.getMessage());
				ret = -1;
				// e.printStackTrace();
			}
		}

		return ret;
	} // setMigScriptLock
	
	public boolean uptPreMigInfo(Connection conn, String script_type, String id, int multi_index) {
		boolean isErr = false;
		
		// status �κ� ����ó�� �ʿ� idx_cnt >= 2 �̻��϶�
		StringBuilder query = new StringBuilder("update /* SCRIPT006 */ ");
		query.append(this.rule_tab_nm).append(" set exec_node=?, status='ING', stime=sysdate \n");
		// query.append("where script_type = ? and owner=? and name = ? "); //and status is null");
		query.append("where script_type = ? and script_no = ? "); //and status is null");
		
		if(multi_index == 0 ) {
			query.append("and status = 'READY'");
		} else {
			query.append("and status = 'LOCK'");
		}
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, id);
			pstmt.setString(2, script_type);
			pstmt.setInt(3, this.script_no);
			
			// pstmt.setString(3, this.owner);
			// pstmt.setString(4, this.name);
			
			count = pstmt.executeUpdate();
			//conn.commit();			
			
		} catch (Exception e) {
			// e.printStackTrace();
			isErr = true;
			Log.error("error in uptPreMigInfo1");
			Log.error(e.getMessage());
			count = 0;
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in uptPreMigInfo2");
				Log.error(e.getMessage());
				// e.printStackTrace();
			}
		}

		return count>0 ? true:false ;
	}
	
	public boolean uptPreMigInfoMI(Connection conn, String script_type) {
		boolean isErr = false;
		// status �κ� ����ó�� �ʿ� idx_cnt >= 2 �̻��϶�
		// ��Ƽ�� ������ ������ ��� ����ó���� ���ؼ��� idx_cnt ���� 1�� ���� �ʿ�, �ٸ� �۷ι��ε��� ���� �ÿ��� ����Ұ�
		// ��Ƽ�����̺� �۷ι��ε���, �����ε��� ���� ��, ��������� �����ε����Ϸ� -> �۷ι��ε����Ϸ� ��������� �����ʿ�
		StringBuilder query = new StringBuilder("update /* SCRIPT007 */ ");
		query.append(this.rule_tab_nm).append(" set status='LOCK' \n");
		
		// �ε����� �ش� �Լ� ȣ��
		query.append("where script_type = ? and table_owner=? and table_nm = ? and part_name is null and status = 'READY' ");
		//query.append("where script_type = ? and table_owner=? and table_nm = ? and status = 'READY' and part_name is not null "); 
		
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, script_type);
			pstmt.setString(2, this.table_owner);
			pstmt.setString(3, this.table_nm);
			
			count = pstmt.executeUpdate();
			// conn.commit();
			
		} catch (Exception e) {
			isErr = true;
			// e.printStackTrace();
			Log.error("error in uptPreMigInfoMI1");
			Log.error(e.getMessage());
			count = 0;
		} finally {
			try {
				if(isErr) { conn.rollback(); } else { conn.commit(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (Exception e) {
				Log.error("error in uptPreMigInfoMI2");
				Log.error(e.getMessage());
				// e.printStackTrace();
			}
		}

		return count>0 ? true:false ;
	}
	
	
	public boolean uptPostMigInfo(Connection conn, String script_type, long elapsed_ms) {
		boolean isErr = false;
		
		StringBuilder query = new StringBuilder("update /* SCRIPT008 */ ");
		query.append(this.rule_tab_nm);
		query.append(" set status='END', etime=sysdate, elapsed_ms=?  \n");
		query.append("where script_type = ? and script_no = ?");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;
		int count = 0;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setLong(1, elapsed_ms);
			pstmt.setString(2, script_type);
			pstmt.setInt(3, this.script_no);
			//pstmt.setString(3, this.owner);
			//pstmt.setString(4, this.name);
			count = pstmt.executeUpdate();
			//conn.commit();			
			
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
	
	public String get_ddl(String script_type, String script_home) {
		StringBuilder logfile = new StringBuilder(script_home);
		logfile.append("/").append(script_path);
		
		Log.info("DDL File : " + logfile.toString());

		String line;
		StringBuilder ddl = new StringBuilder("");
		
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(logfile.toString()));
			while((line = br.readLine()) != null) {
				// System.out.println(line);
				// if("/".equals(line.trim()) && !this.ETC.equals(script_type)) {  
				if("/".equals(line.trim()) ) {	
					// slash ó��, ���ϳ����� tbsql ����� �ʿ�������, java������ ���ʿ� 
					// null üũ while ������ ����üũ�ؼ� trim ó���Ͽ���
					continue;
				} else if (line.contains("SHOW ERRORS;") && this.ETC.equals(script_type)) { 
					// pl/sql  ���� SHOW ERRORS; ���� �� skip ó��, 
					continue;
					
				} else {
					ddl.append("\n").append(line);
				}
				
			}
		} catch (FileNotFoundException e) {
			Log.error("DDL File Not Found : "+logfile.toString());
			Log.error(e.getMessage());
		} catch (Exception e) {
			Log.error("get_ddl error");
			Log.error(e.getMessage());
		}	
		
		Log.debug("DDL : \n" + ddl.toString() );
		return ddl.toString();
	}
	
	public String get_ddl(Connection conn) {

		StringBuilder ddl = new StringBuilder("");
		
		
		StringBuilder query1 = new StringBuilder("select /* SCRIPT010 */ constraint_name from dba_constraints  \n");
		query1.append(" where status = 'DISABLED' and owner =  ? and table_name =  ? \n");
		
		Log.trace("query1 : \n"+ query1.toString());
		
		PreparedStatement pstmt = null;
		ResultSet rs1 = null;
		
		//boolean bRet = false;
		// 0:������ȸ, 1:������, 100:�����߻�  
		//int iRet=0;
		
		
		try {
			Log.trace("get_ddl : select disabled constraints");
			pstmt = conn.prepareStatement(query1.toString());
			pstmt.setString(1, this.table_owner);
			pstmt.setString(2, this.table_nm);
			rs1 = pstmt.executeQuery();
			
			String con_nm;
			String enable_clause = "";
			
			if(this.DDEnb.equalsIgnoreCase(this.script_path)) {
				enable_clause = " enable constraint ";
			} else if(this.DDEnbNo.equalsIgnoreCase(this.script_path)) {
				enable_clause = " enable novalidate constraint ";
			} else {
				// �ش� else �������� ������ �ȵǴ� �κ��̸�, �߻� �� ������� �κ�
				Log.error("Invalid Value(script_path) : " + this.script_path);
			}
			
			while(rs1.next()) {
				con_nm = rs1.getString("CONSTRAINT_NAME");
				
				ddl.append("alter table ").append(this.table_owner).append(".").append(this.table_nm);
				ddl.append(enable_clause).append(con_nm).append(";\n");
			}
			
			// ��� �� ���� �� �α�ó��
			if( "".equalsIgnoreCase(ddl.toString() ) ) {
				Log.warn("Not Exists Disabled Constraints : " + this.table_owner +"."+ this.table_nm);
			}
		
			
		} catch (SQLException e) {
			Log.error(e.getMessage());
			//iRet = 100;
		} finally {
			try {
				if (null != rs1) { rs1.close(); }
				if (null != pstmt) { pstmt.close(); }
			} catch (SQLException e) {
				Log.error(e.getMessage());
			}
		}
		
		Log.debug("DDL : \n" + ddl.toString() );
		
		// ��� �� ���� �� �ܺο��� "" ������ ����ó�� �ʿ���
		return ddl.toString();
	}
 
	public String get_ddl() { // �ε��� ������ ��� ��� ��

		StringBuilder ddl = new StringBuilder("ALTER INDEX ");
		ddl.append(owner).append(".").append(name).append(" REBUILD");
		
		// ��Ƽ�� ������ ���� ��
		if(this.part_name != null && !"".equals(this.part_name.trim())) {
			
			// ������Ƽ�� ó��
			if( this.SubPartition.equalsIgnoreCase(part_type)) {
				ddl.append(" SUBPARTITION ").append(this.part_name);
			} else if( this.Partition.equalsIgnoreCase(part_type)) {
				ddl.append(" PARTITION ").append(this.part_name);
			} else {
				// ddl.append(" PARTITION ").append(this.part_name);
				Log.warn("mismatch partition_name and partition_type : " + this.script_no);
			}
			
		}
		
		// �з��� ��� ��� ��
		if( this.degree >= 2 && this.degree <= 256 ) {
			ddl.append(" PARALLEL ").append(this.degree);
		}
		
		Log.info("Index ReBuild DDL : \n" + ddl.toString() );
		
		// ��� �� ���� �� �ܺο��� "" ������ ����ó�� �ʿ���
		return ddl.toString();
	}
	
	public long execute_ddl(Connection conn, String script_type, String ddl, String rule_schema, String switch_owner_schema) {
		
		Statement stmt = null;
		boolean ret = true;
		
		// �����޽��� �ʱ�ȭ
		this.errMsg = "";
		
		// ����ð� ����
		long startTime = System.currentTimeMillis();
		
		/* �����ʿ�
		 * SWITCH_OWNER_SCHEMA ��  ALTER SESSION SET CURRENT_SCHEMA=#OWNER
		 * degree ��� �� ALTER SESSION FORCE PARALLEL DDL PARALLEL 8/ALTER SESSION FORCE PARALLEL QUERY PARALLEL 8
		 */
		
		try {
			
			stmt = conn.createStatement();
			
			// current schema ����ó��, �ε��������� ��� ���� �������� �־ ó���ϹǷ� ����
			if("Y".equalsIgnoreCase(switch_owner_schema) && !(this.INDEX.equals(script_type) && DDRebuild.equalsIgnoreCase(this.script_path)) ) {
				Log.debug("switch owner schema : "+ this.owner);
				stmt.addBatch("alter session set current_schema ="+this.owner);
			}
			
			// degree ó��(index, constraint��), table�� ���� ���� �ξ��µ�, CTAS �����϶� �߰����� ����ʿ�
			/* 
			 * 1. �ε��� and ������ ���°� �ƴϸ鼭, degree ������ ���
			 * 2. �������� �̸鼭 degree ������ ���
			 */
			if( ( !(this.INDEX.equals(script_type) && DDRebuild.equalsIgnoreCase(this.script_path)) || this.CONSTRAINT.equals(script_type)) 
					&& this.degree >= 2 && this.degree <= 256) {
				/* 
				 * alter session force parallel ddl parallel #degree
				 * alter session force parallel query parallel #degree
				 */

				//Log.debug("degree : "+ this.degree);
				Log.debug("parallel execution ["+this.owner +"."+this.name +"] - degree : "+ this.degree);
				stmt.addBatch("alter session force parallel ddl parallel "+this.degree);
				stmt.addBatch("alter session force parallel query parallel "+this.degree);
				
			}
			
			// Constraint�� ��� ���ջ����� ���� �����ؼ� loop ���鼭 ó�� �ʿ�
			if( this.CONSTRAINT.equals(script_type) && ddl != null ) { // ���ջ������� ���
				String[] multiddl = ddl.split(";");
				if(multiddl.length > 0 ) {
					for(int i=0; i<multiddl.length; i++ ) {
						
						if( !"".equals(multiddl[i].trim()) ) { // ����� ó��
							stmt.addBatch(multiddl[i]);
						}
							
					} // for end
					
				} else {  // �ش� ������ Ÿ�� ��찡 �߻��ϸ� �ȵ�
					throw new Exception("Internal Error : Constraint DDL Split Error");  
				}
			} else { // Constratint �̿ܿ� ó��
				stmt.addBatch(ddl);
			}
			
			
			
			stmt.executeBatch();
			
		} catch (SQLException e) {
			Log.error("[SQLException] execute ddl fail : "+ ddl);
			Log.error(e.getMessage());
			this.errMsg = e.getMessage();
			ret = false;
		} catch (Exception e) {
			Log.error("[Exception] execute ddl fail : "+ ddl);
			Log.error(e.getMessage());
			this.errMsg = e.getMessage();
			ret = false;
		} finally {
			try {
				// ��ó�� ��� �ε��� �����ܰ迡�� ���� ���� ��� ������ ���� �����Ƿ� ���⼭ ó��
				/* ���������� ���� ó�� */
				stmt.clearBatch();
				
				// degree ó��
				if( ( !(this.INDEX.equals(script_type) && DDRebuild.equalsIgnoreCase(this.script_path)) || this.CONSTRAINT.equals(script_type)) 
						&& this.degree >= 2 && this.degree <= 256 ) {
					/* 
					 * alter session force parallel ddl parallel #degree
					 * alter session force parallel query parallel #degree
					 */
					Log.debug("alter session disable parallel/ddl");
					stmt.addBatch("alter session disable parallel ddl");
					stmt.addBatch("alter session disable parallel query");
					
				}
				
				
				if("Y".equals(switch_owner_schema) && !(this.INDEX.equals(script_type) && DDRebuild.equalsIgnoreCase(this.script_path)) ) {
					Log.debug("switch rule schema : "+ rule_schema);
					stmt.addBatch("alter session set current_schema ="+rule_schema);
				}
				
				
				stmt.executeBatch();
			
				if (null != stmt) { stmt.close(); }
				
			} catch (Exception e) {
				Log.error("error in execute_ddl");
				Log.error(e.getMessage());
				this.errMsg = e.getMessage();
				ret = false;
			} 
		}
		
		long endTime = System.currentTimeMillis();
		
		return ret ? endTime-startTime:-1;
	}
	
	
	public void setErr(Connection conn, String script_type, String status, String err_msg) {
		boolean isErr = false;
		
		StringBuilder query = new StringBuilder("update /* SCRIPT009 */ ");
		query.append(this.rule_tab_nm).append(" set status=?, err_msg=?, etime=sysdate\n");
		query.append("where script_type = ? and script_no = ? ");
		
		Log.trace("query : \n"+ query.toString());
		
		PreparedStatement pstmt = null;

		try {
			pstmt = conn.prepareStatement(query.toString());
			pstmt.setString(1, status);
			pstmt.setString(2, err_msg.length()<=255 ? err_msg:err_msg.substring(0, 255));
			
			pstmt.setString(3, script_type);
			pstmt.setInt(4, this.script_no);
			//pstmt.setString(3, this.owner);
			//pstmt.setString(4, this.name);
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
		} // finally

	}
}
