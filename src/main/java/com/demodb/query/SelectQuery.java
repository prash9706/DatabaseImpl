package com.demodb.query;

import java.io.File;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.util.Constants;

public class SelectQuery implements Query {
	private String tableName;
	private String[] columns;
	private String[] conditions;
	
	public SelectQuery(String tableName, String[] columns, String[] conditions){
		this.tableName = tableName;
		this.columns = columns;
		this.conditions = conditions;
	}
	
	@Override
	public ResultSet execute() {
		DatabaseHelper.select(this.tableName, this.columns, this.conditions, true);
		ResultSet rs = new ResultSet(0);
		rs.setData("");
		return rs;
	}

	@Override
	public Message isValid() {
		Message res = null;
		if(this.tableName==null || this.tableName.isEmpty()){
			res = new Message("Empty table name");
		}else if(!isTableExist()){
			res = new Message("Table with name "+this.tableName+" does not exists");
		}
		return res;
	}
	
	private boolean isTableExist(){
		String dbTable = this.tableName+".tbl";
		try {
			File data_dir = new File(Constants.dirUserdata);
			if (dbTable.equalsIgnoreCase(Constants.TABLE_CATALOG+Constants.FILE_TYPE) || dbTable.equalsIgnoreCase(Constants.COLUMN_CATALOG+Constants.FILE_TYPE))
				data_dir = new File(Constants.dirCatalog) ;
			String[] oldTables = data_dir.list();
			for (int i=0; i<oldTables.length; i++) {
				if(oldTables[i].equals(dbTable))
					return true;
			}
		}
		catch (Exception e) {
			System.out.println("Unable to create directory");
			System.out.println(e);
		}
		return false;
	}
}
