package com.demodb.query;

import java.io.File;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.util.Constants;

public class InsertQuery implements Query {
	private String tableName;
	private String[] valuess;
	
	public InsertQuery(String tableName, String values[]){
		this.tableName = tableName;
		this.valuess = values;
	}
	
	@Override
	public ResultSet execute() {
		ResultSet rs = null;
		if(DatabaseHelper.insertInto(this.tableName, this.valuess, 
				Constants.dirUserdata+Constants.SEP)){
			rs = new ResultSet(1);
			rs.setData("Record inserted successfully ");
		}else{
			rs = new ResultSet(0);
			rs.setData("Failed to insert Record");
		}
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
