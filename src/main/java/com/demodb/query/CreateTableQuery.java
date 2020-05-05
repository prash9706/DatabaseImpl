package com.demodb.query;

import java.io.File;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.util.Constants;

public class CreateTableQuery implements Query {
	private String tableName;
	private String[] cols;
	
	public CreateTableQuery(String tableName, String[] cols) {
		this.tableName = tableName;
		this.cols = cols;
	}
	@Override
	public ResultSet execute() {
		boolean isCreated = DatabaseHelper.createTable(this.tableName, this.cols);
		ResultSet rs = new ResultSet(1);
		if(isCreated){
			rs.setData("Table "+this.tableName+" created successfully");
		}else{
			rs.setData("Failed to create table "+this.tableName);
		}
		return rs;
	}

	@Override
	public Message isValid() {
		Message res = null;
		if(this.tableName==null || this.tableName.isEmpty()){
			res = new Message("Empty table name");
		}else if(this.cols ==null || cols.length==0){
			res = new Message("Empty set of columns");
		}else if(isTableExist()){
			res = new Message("Table with name "+this.tableName+" already exists");
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
