package com.demodb;

import static java.lang.System.out;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import com.demodb.util.Constants;

/**
 * @author Chris Irwin Davis
 * @version 1.0 <b>
 *          <p>
 * 			This is an example of how to create an interactive prompt
 *          </p>
 *          <p>
 * 			There is also some guidance to get started wiht read/write of binary
 *          data files using RandomAccessFile class
 *          </p>
 *          </b>
 *
 */
public class DavisBasePrompt {
	static boolean isExit = false;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/*
		 * This method will initialize the database storage if it doesn't exit
		 */
		DavisBaseBinaryFileExample.initializeDataStore();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		while (!isExit) {
			System.out.print(Constants.PROMPT);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}

	/**
	 * ***********************************************************************
	 * Static method definitions
	 */

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself
	 *         num times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("CREATE DATABASE <database_name>;");
		out.println("\tCreate database");
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

	/** return the DavisBase version */
	public static String getVersion() {
		return Constants.VERSION;
	}

	public static String getCopyright() {
		return Constants.COPYRIGHT;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		/*
		 * commandTokens is an array of Strings that contains one token per
		 * array element The first token can be used to determine the type of
		 * command The other tokens can be used to pass relevant parameters to
		 * each command-specific method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		/*
		 * This switch handles a very small list of hardcoded commands of known
		 * syntax. You will want to rewrite this method to interpret more
		 * complex commands.
		 */
		switch (commandTokens.get(0)) {
		case "select":
			System.out.println("CASE: SELECT");
			parseQuery(userCommand);
			break;
		case "drop":
			System.out.println("CASE: DROP");
			dropTable(userCommand);
			break;
		case "create":
			System.out.println("CASE: CREATE");
			parseCreateTable(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	/**
	 * Stub method for dropping tables
	 * 
	 * @param dropTableString
	 *            is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		System.out.println("STUB: This is the dropTable method.");
		System.out.println("\tParsing the string:\"" + dropTableString + "\"");
	}

	/**
	 * Stub method for executing queries
	 * 
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseQuery(String queryString) {
		System.out.println("STUB: This is the parseQuery method");
		System.out.println("\tParsing the string:\"" + queryString + "\"");
	}

	/**
	 * Stub method for creating new tables
	 * 
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {

		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		/* Define table file name */
		String tableFileName = createTableTokens.get(2) + ".tbl";

		/* YOUR CODE GOES HERE */

		/* Code to create a .tbl file to contain table data */
		try {
			/*
			 * Create RandomAccessFile tableFile in read-write mode. Note that
			 * this doesn't create the table file in the correct directory
			 * structure
			 */
			RandomAccessFile tableFile = new RandomAccessFile(tableFileName, "rw");
			tableFile.setLength(Constants.PAGE_SIZE);
			tableFile.seek(0);
			tableFile.writeInt(63);
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}