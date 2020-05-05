package com.demodb.query;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.util.Constants;

public class ShowTablesQuery implements Query {
	String table = Constants.TABLE_CATALOG;
	String[] cols = {Constants.HEADER_TABLE_NAME};
	
	@Override
	public ResultSet execute() {
		String[] cols = {Constants.HEADER_TABLE_NAME};
		String[] condition = new String[0];
		DatabaseHelper.select(Constants.TABLE_CATALOG, cols, condition,true);
		ResultSet rs = new ResultSet(0);
		rs.setData("");
		return rs;
	}

	@Override
	public Message isValid() {
		return null;
	}

}
