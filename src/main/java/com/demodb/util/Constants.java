package com.demodb.util;

public class Constants {
	
	public static final String SELECT_COMMAND = "SELECT";
    public static final String DROP_TABLE_COMMAND = "DROP TABLE";
    public static final String HELP_COMMAND = "HELP";
    public static final String VERSION_COMMAND = "VERSION";
    public static final String EXIT_COMMAND = "EXIT";
    public static final String QUITE_COMMAND = "QUITE";
    public static final String SHOW_TABLES_COMMAND = "SHOW TABLES";
    public static final String INSERT_COMMAND = "INSERT INTO";
    public static final String CREATE_TABLE_COMMAND = "CREATE TABLE";
    public static final String dirUserdata = "data/user_data/";
    public static final String dirCatalog = "data/catalog/";
    public static final String TABLE_CATALOG = "davisbase_tables";
	public static final String COLUMN_CATALOG = "davisbase_columns";
	public final static String SEP = "/";
	public final static String DEFAULT_DATA_DIR = "data";
	public final static String PROMPT = "davisql> ";
	public final static String VERSION = "v1.0.0";
	public final static String COPYRIGHT = "Â©2016 Chris Irwin Davis";
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	
	public final static int PAGE_SIZE = 512;
	public static final int RECORDS_PAGE = 0x0D;
	//NULL
	public static final byte NULL = 0x00;
	public static final byte SHORTNULL = 0x01;
	public static final byte INTNULL = 0x02;
	public static final byte LONGNULL = 0x03;
	
	//Numeric
	public static final byte TINYINT = 0x04;
	public static final byte SHORTINT = 0x05;
	public static final byte INT = 0x06;
	public static final byte LONG = 0x07;
	public static final byte FLOAT = 0x08;
	public static final byte DOUBLE = 0x09;
	
	//DateTime
	public static final byte DATETIME = 0x0A;
	public static final byte DATE = 0x0B;
	
	//TEXT
	public static final byte TEXT = 0x0C;
	public static final int TABLE_OFFSET = PAGE_SIZE - 24;
	public static final int COLUMN_OFFSET = TABLE_OFFSET - 25;
	
	//math operators
	public static final String EQUALS_SIGN = "=";
	public static final String LESS_THAN_SIGN = "<";
	public static final String GREATER_THAN_SIGN = ">";
	public static final String LESS_THAN_EQUAL_SIGN = "<=";
	public static final String GREATER_THAN_EQUAL_SIGN = ">=";
	public static final String NOT_EQUAL_SIGN = "!=";

	//file type
	public static final String FILE_TYPE = ".tbl";
	public static final String INDEX_FILE_TYPE = ".ndx";
	
	//table headers
	public static final String HEADER_ROWID = "rowid";
	public static final String HEADER_TABLE_NAME = "table_name";
	public static final String HEADER_TEXT = "TEXT";
	public static final String HEADER_IS_UNIQUE = "is_unique";
	public static final String HEADER_IS_NULLABLE = "is_nullable";
	
	//boolean strings
	public static final String FALSE = "NO";
	public static final String TRUE = "TRUE";
}
