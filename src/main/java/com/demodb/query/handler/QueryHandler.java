package com.demodb.query.handler;

import static java.lang.System.out;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;
import com.demodb.query.CreateTableQuery;
import com.demodb.query.DropTableQuery;
import com.demodb.query.InsertQuery;
import com.demodb.query.Query;
import com.demodb.query.SelectQuery;
import com.demodb.query.ShowTablesQuery;
import com.demodb.util.ConsoleWriter;
import com.demodb.util.Constants;

public class QueryHandler {

	public static boolean handle(String userCommand) {
		boolean isExit = false;
		int queryType = detectQuery(userCommand);
		switch (queryType) {
		case 1:
			executeCreateTable(userCommand);
			break;
		case 2:
			 executeInsert(userCommand);
			break;
		case 3:
			 executeSelect(userCommand);
			break;
		case 4:
			 executeShowTable(userCommand);
			break;
		case 5:
			 executeDropTable(userCommand);
			break;
		case 6:
			isExit = true;
			break;
		case 7:
			displayVersion();
			break;
		case 8:
			help();
			break;
		default:
			ConsoleWriter.write("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
		return isExit;
	}

	private static void executeSelect(String userCommand) {
		String tableName = QueryUtil.getTableNameFromSelect(userCommand);
		String columns[] = QueryUtil.getColumnsFromSelect(userCommand);
		String conditions[] = QueryUtil.getConditionsFromSelect(userCommand);
		Query selectQuery = new SelectQuery(tableName, columns, conditions);
		Message msg = selectQuery.isValid();
		if(msg==null){
			ResultSet rs = selectQuery.execute();
			ConsoleWriter.write(rs);
		}else{
			ConsoleWriter.write(msg);
		}
	}

	private static void executeInsert(String userCommand) {
		String tableName = QueryUtil.getTableNameFromInsert(userCommand);
		String columnString = QueryUtil.getColumnsString(userCommand);
		String[] columns = columnString.split(",");
		String[] dbColumns = new String[columns.length + 1];
		for(int i = 1; i <= columns.length; i++)
			dbColumns[i] = columns[i-1].trim();
		Query insertQuery = new InsertQuery(tableName, dbColumns);
		Message msg = insertQuery.isValid();
		if(msg==null){
			ResultSet rs = insertQuery.execute();
			ConsoleWriter.write(rs);
		}else{
			ConsoleWriter.write(msg);
		}
	}

	private static void executeDropTable(String userCommand) {
		String tableName = QueryUtil.getTableNameFromDropQuery(userCommand);
		Query dropQuery = new DropTableQuery(tableName);
		Message msg = dropQuery.isValid();
		if(msg==null){
			ResultSet rs = dropQuery.execute();
			ConsoleWriter.write(rs);
		}else{
			ConsoleWriter.write(msg);
		}
	}

	private static void executeShowTable(String userCommand) {
		Query showQuery = new ShowTablesQuery();
		showQuery.execute();
	}

	private static void executeCreateTable(String userCommand) {
		String tableName = QueryUtil.getTableNameFromQuery(userCommand);
		String cols = QueryUtil.getColumnsString(userCommand);
		String[] create_cols = cols.split(",");
		for(int i = 0; i < create_cols.length; i++)
			create_cols[i] = create_cols[i].trim();
		Query createQuery = new CreateTableQuery(tableName, create_cols);
		Message msg = createQuery.isValid();
		if (msg != null) {
			ConsoleWriter.write(msg);
			return;
		}
		ResultSet rs = createQuery.execute();
		ConsoleWriter.write(rs);
	}

	private static int detectQuery(String userCommand) {
		int i = -1;
		if (userCommand == null || userCommand.isEmpty()) {
			return i;
		}
		if (userCommand.toLowerCase().startsWith(Constants.CREATE_TABLE_COMMAND.toLowerCase())) {
			i = 1;
		} else if (userCommand.toLowerCase().startsWith(Constants.INSERT_COMMAND.toLowerCase())) {
			i = 2;
		} else if (userCommand.toLowerCase().startsWith(Constants.SELECT_COMMAND.toLowerCase())) {
			i = 3;
		} else if (userCommand.toLowerCase().startsWith(Constants.SHOW_TABLES_COMMAND.toLowerCase())) {
			i = 4;
		} else if (userCommand.toLowerCase().startsWith(Constants.DROP_TABLE_COMMAND.toLowerCase())) {
			i = 5;
		}else if (userCommand.toLowerCase().startsWith(Constants.EXIT_COMMAND.toLowerCase())
				|| userCommand.toLowerCase().startsWith(Constants.QUITE_COMMAND.toLowerCase())) {
			i = 6;
		} else if (userCommand.toLowerCase().startsWith(Constants.VERSION_COMMAND.toLowerCase())) {
			i = 7;
		} else if (userCommand.toLowerCase().startsWith(Constants.HELP_COMMAND.toLowerCase())) {
			i = 8;
		}
		return i;
	}

	public static void displayVersion() {
		ConsoleWriter.write("DavisBaseLite Version " + getVersion());
		ConsoleWriter.write(getCopyright());
	}

	public static String getVersion() {
		return Constants.VERSION;
	}

	public static String getCopyright() {
		return Constants.COPYRIGHT;
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("INSERT INTO <table_name> VALUES <values>;");
		out.println("\tInsert values into table\n");
		out.println("SELECT * FROM <table_name>;");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}
}
