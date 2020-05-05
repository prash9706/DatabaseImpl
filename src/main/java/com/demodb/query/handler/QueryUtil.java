package com.demodb.query.handler;

import com.demodb.util.Constants;

public class QueryUtil {
	
	public static String getTableNameFromQuery(String query){
		String tableName = "";
		if(query==null || query.isEmpty()){
			return tableName;
		}
		int idx = query.indexOf('(');
		query = query.substring(0, idx);
		String querySplit[] = Constants.CREATE_TABLE_COMMAND.split(" ");
		String userQuerySplit[] = query.split(" ");
		if(userQuerySplit.length<=querySplit.length){
			return tableName;
		}
		return userQuerySplit[2].trim();
	}
	
	public static String getColumnsString(String userQuery){
		String cols = "";
		int idx1 = userQuery.indexOf("(");
		int idx2 = userQuery.indexOf(")");
		if(idx1<=0 || idx2<=0){
			return cols;
		}
		cols = userQuery.substring(idx1+1, idx2);
		return cols.trim();
	}
	
	public static String getTableNameFromDropQuery(String query){
		String tableName = "";
		if(query==null || query.isEmpty()){
			return tableName;
		}
		String querySplit[] = Constants.DROP_TABLE_COMMAND.split(" ");
		String userQuerySplit[] = query.split(" ");
		if(userQuerySplit.length<=querySplit.length){
			return tableName;
		}
		return userQuerySplit[2].trim();
	}
	
	public static String getTableNameFromInsert(String query){
		String tableName = "";
		if(query==null || query.isEmpty()){
			return tableName;
		}
		int idx = query.indexOf("(");
		query = query.substring(0, idx);
		String querySplit[] = Constants.INSERT_COMMAND.split(" ");
		String userQuerySplit[] = query.split(" ");
		if(userQuerySplit.length<=querySplit.length){
			return tableName;
		}
		return userQuerySplit[2].trim();
	}
	
	public static String getTableNameFromSelect(String query){
		String tableName = "";
		if(query==null || query.isEmpty()){
			return tableName;
		}
		int idx = query.toLowerCase().indexOf("from");
		if(idx<=0){
			return tableName;
		}
		String temp = query.substring(idx+"from".length());
		return temp.split(" ")[1].trim();		
	}
	
	public static String[] getColumnsFromSelect(String query) {
		String columns[] = new String[1];
		columns[0] = "*";
		if(query==null || query.isEmpty()){
			return columns;
		}
		int idx = query.toLowerCase().indexOf("from");
		if(idx<=0){
			return columns;
		}
		String temp = query.substring(0,idx);
		if(temp.contains("*")){
			return columns;
		}
		String cols = temp.toLowerCase().replace("select", "").trim();
		columns = cols.split(",");
		for(int i=0;i<columns.length;i++){
			columns[i] = columns[i].trim();
		}
		return columns;
	}
	
	public static String[] getConditionsFromSelect(String query) {
		String conditions[] = new String[0];
		if(query==null || query.isEmpty()){
			return conditions;
		}
		int idx = query.toLowerCase().indexOf("where");
		if(idx<=0){
			return conditions;
		}
		String temp = query.substring(idx+"where".length()).trim();
		return parseCondition(temp);
	}

	
	public static String[] parseCondition(String condition){
		String parsedCondition[] = new String[3];
		String temp[] = new String[2];
		if(condition.contains(Constants.EQUALS_SIGN)) {
			temp = condition.split(Constants.EQUALS_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.EQUALS_SIGN;
			parsedCondition[2] = temp[1].trim();
		}
		
		if(condition.contains(Constants.LESS_THAN_SIGN)) {
			temp = condition.split(Constants.LESS_THAN_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.LESS_THAN_SIGN;
			parsedCondition[2] = temp[1].trim();
		}
		
		if(condition.contains(Constants.GREATER_THAN_SIGN)) {
			temp = condition.split(Constants.GREATER_THAN_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.GREATER_THAN_SIGN;
			parsedCondition[2] = temp[1].trim();
		}
		
		if(condition.contains(Constants.LESS_THAN_EQUAL_SIGN)) {
			temp = condition.split(Constants.LESS_THAN_EQUAL_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.LESS_THAN_EQUAL_SIGN;
			parsedCondition[2] = temp[1].trim();
		}

		if(condition.contains(Constants.GREATER_THAN_EQUAL_SIGN)) {
			temp = condition.split(Constants.GREATER_THAN_EQUAL_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.GREATER_THAN_EQUAL_SIGN;
			parsedCondition[2] = temp[1].trim();
		}
		
		if(condition.contains(Constants.NOT_EQUAL_SIGN)) {
			temp = condition.split(Constants.NOT_EQUAL_SIGN);
			parsedCondition[0] = temp[0].trim();
			parsedCondition[1] = Constants.NOT_EQUAL_SIGN;
			parsedCondition[2] = temp[1].trim();
		}

		return parsedCondition;
	}
	
	
	public static void main(String[] args){
		String userCommand = "create table student(id int, name text)";
		String dropTableCommand = "Drop table student";
		String insertCommand = "INSERT INTO table_name VALUES (value1, value2, value3);";
		String selectCommand = "Select * from table";
		String selectColCommand = "Select col1, col2, col3 from table";
		String selectWithWhere = "Select col1, col2, col3 from table where col1=val";
		System.out.println(getTableNameFromQuery(userCommand));
		System.out.println(getColumnsString(userCommand));
		System.out.println(getTableNameFromDropQuery(dropTableCommand));
		System.out.println(getTableNameFromInsert(insertCommand));
		System.out.println(getColumnsString(insertCommand));
		System.out.println(getTableNameFromSelect(selectColCommand));
		for(String col:getColumnsFromSelect(selectColCommand)){
			System.out.print(col+" ");
		}
		System.out.println(getTableNameFromSelect(selectWithWhere));
		for(String col:getColumnsFromSelect(selectWithWhere)){
			System.out.print(col+" ");
		}
		for(String condtions:getConditionsFromSelect(selectWithWhere)){
			System.out.println(condtions+" ");
		}
	}

}
