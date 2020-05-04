package com.demodb.query;

import java.io.File;
import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.util.ConsoleWriter;
import com.demodb.util.Constants;

public class CreateDBQuery implements Query {
	
	private String dbName;
	
	public CreateDBQuery(String dbName){
		this.dbName = dbName;
	}
	
	@Override
	public ResultSet execute() {
		File file = new File(getTargetDir());
		file.mkdir();
		ResultSet resultSet = new ResultSet(1);
		resultSet.setData(this.dbName + "database successfully created");
		return resultSet;
	}

	@Override
	public Message isValid() {
		Message res = null;
		File dir = new File(getTargetDir());
		if(dir.exists()){
			res = new Message("Database already exist");
		}
		return res;
	}
	
	@Override
	public void displayContent(ResultSet resultSet){
		ConsoleWriter.write(resultSet);
	}
	
	private String getTargetDir(){
		return Constants.DEFAULT_DATA_DIR + Constants.SEP + this.dbName;
	}

}
