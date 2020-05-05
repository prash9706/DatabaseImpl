package com.demodb;

import java.io.File;
import java.io.RandomAccessFile;

import com.demodb.query.DatabaseHelper;
import com.demodb.util.Constants;

public class DavisBaseInitiallizer {
	
	public static void init(){
		try {
			File data_dir = new File("data");
			if(data_dir.mkdir()){
				System.out.println("Initializing...");
				initialize();
			}
			else {
				data_dir = new File(Constants.dirCatalog);
				String[] oldTables = data_dir.list();
				boolean tableExists = false;
				boolean colExists = false;
				for (int i=0; i<oldTables.length; i++) {
					if(oldTables[i].equals(Constants.TABLE_CATALOG+Constants.FILE_TYPE))
						tableExists = true;
					if(oldTables[i].equals(Constants.COLUMN_CATALOG+Constants.FILE_TYPE))
						colExists = true;
				}
				
				if(!tableExists){
					System.out.println("Davisbase_tables does not exist, initializing...");
					System.out.println();
					initialize();
				}
				
				if(!colExists){
					System.out.println("Davisbase_columns table does not exist, initializing...");
					System.out.println();
					initialize();
				}
				
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}

	}
	
public static void initialize() {

		
		try {
			File data_dir = new File(Constants.dirUserdata);
			data_dir.mkdir();
			data_dir = new File(Constants.dirCatalog);
			data_dir.mkdir();
			String[] oldTables;
			oldTables = data_dir.list();
			for (int i=0; i<oldTables.length; i++) {
				File oldFile = new File(data_dir, oldTables[i]); 
				oldFile.delete();
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile catalogTable = new RandomAccessFile(Constants.dirCatalog+"/davisbase_tables.tbl", "rw");
			catalogTable.setLength(Constants.PAGE_SIZE);
			catalogTable.seek(0);
			catalogTable.write(0x0D);
			catalogTable.writeByte(0x02);
									
			//creating davisbase_tables
			catalogTable.writeShort(Constants.COLUMN_OFFSET);
			catalogTable.writeInt(0);
			catalogTable.writeInt(0);
			catalogTable.writeShort(Constants.TABLE_OFFSET);
			catalogTable.writeShort(Constants.COLUMN_OFFSET);
			
			catalogTable.seek(Constants.TABLE_OFFSET);
			catalogTable.writeShort(20);
			catalogTable.writeInt(1); 
			catalogTable.writeByte(1);
			catalogTable.writeByte(28);
			catalogTable.writeBytes(Constants.TABLE_CATALOG);
			
			catalogTable.seek(Constants.COLUMN_OFFSET);
			catalogTable.writeShort(21);
			catalogTable.writeInt(2); 
			catalogTable.writeByte(1);
			catalogTable.writeByte(29);
			catalogTable.writeBytes(Constants.COLUMN_CATALOG);
			
			catalogTable.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
		
		try {
			RandomAccessFile catalogColumn = new RandomAccessFile(Constants.dirCatalog+"/davisbase_columns.tbl", "rw");
			catalogColumn.setLength(Constants.PAGE_SIZE);
			catalogColumn.seek(0);       
			catalogColumn.writeByte(0x0D); 
			catalogColumn.writeByte(0x09); //no of records

			
			int[] offset=new int[9];
			offset[0]=Constants.PAGE_SIZE-45;
			offset[1]=offset[0]-49;
			offset[2]=offset[1]-46;
			offset[3]=offset[2]-50;
			offset[4]=offset[3]-51;
			offset[5]=offset[4]-49;
			offset[6]=offset[5]-59;
			offset[7]=offset[6]-51;
			offset[8]=offset[7]-49;
			
			catalogColumn.writeShort(offset[8]); 
			catalogColumn.writeInt(0); 
			catalogColumn.writeInt(0); 
			
			for(int i=0;i<offset.length;i++)
				catalogColumn.writeShort(offset[i]);

			
			//creating davisbase_columns
			catalogColumn.seek(offset[0]);
			catalogColumn.writeShort(36);
			catalogColumn.writeInt(1); //key
			catalogColumn.writeByte(6); //no of columns
			catalogColumn.writeByte(28); //16+12next file lines indicate the code for datatype/length of the 5 columns
			catalogColumn.writeByte(17); //5+12
			catalogColumn.writeByte(15); //3+12
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.TABLE_CATALOG); 
			catalogColumn.writeBytes(Constants.HEADER_ROWID); 
			catalogColumn.writeBytes("INT"); 
			catalogColumn.writeByte(1); 
			catalogColumn.writeBytes(Constants.FALSE); 
			catalogColumn.writeBytes(Constants.FALSE); 
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[1]);
			catalogColumn.writeShort(42); 
			catalogColumn.writeInt(2); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(28);
			catalogColumn.writeByte(22);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.TABLE_CATALOG); 
			catalogColumn.writeBytes(Constants.HEADER_TABLE_NAME); 
			catalogColumn.writeBytes(Constants.HEADER_TEXT); 
			catalogColumn.writeByte(2);
			catalogColumn.writeBytes(Constants.FALSE); 
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[2]);
			catalogColumn.writeShort(37); 
			catalogColumn.writeInt(3); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(17);
			catalogColumn.writeByte(15);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes(Constants.HEADER_ROWID);
			catalogColumn.writeBytes("INT");
			catalogColumn.writeByte(1);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[3]);
			catalogColumn.writeShort(43);
			catalogColumn.writeInt(4); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(22);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes(Constants.HEADER_TABLE_NAME);
			catalogColumn.writeBytes(Constants.HEADER_TEXT);
			catalogColumn.writeByte(2);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[4]);
			catalogColumn.writeShort(44);
			catalogColumn.writeInt(5); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(23);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes("column_name");
			catalogColumn.writeBytes(Constants.HEADER_TEXT);
			catalogColumn.writeByte(3);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[5]);
			catalogColumn.writeShort(42);
			catalogColumn.writeInt(6); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(21);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes("data_type");
			catalogColumn.writeBytes(Constants.HEADER_TEXT);
			catalogColumn.writeByte(4);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[6]);
			catalogColumn.writeShort(52); 
			catalogColumn.writeInt(7); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(28);
			catalogColumn.writeByte(19);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes("ordinal_position");
			catalogColumn.writeBytes("TINYINT");
			catalogColumn.writeByte(5);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.seek(offset[7]);
			catalogColumn.writeShort(44); 
			catalogColumn.writeInt(8); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(23);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes(Constants.HEADER_IS_NULLABLE);
			catalogColumn.writeBytes(Constants.HEADER_TEXT);
			catalogColumn.writeByte(6);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
		

			catalogColumn.seek(offset[8]);
			catalogColumn.writeShort(42); 
			catalogColumn.writeInt(9); 
			catalogColumn.writeByte(6);
			catalogColumn.writeByte(29);
			catalogColumn.writeByte(21);
			catalogColumn.writeByte(16);
			catalogColumn.writeByte(4);
			catalogColumn.writeByte(14);
			catalogColumn.writeByte(14);
			catalogColumn.writeBytes(Constants.COLUMN_CATALOG);
			catalogColumn.writeBytes(Constants.HEADER_IS_UNIQUE);
			catalogColumn.writeBytes(Constants.HEADER_TEXT);
			catalogColumn.writeByte(7);
			catalogColumn.writeBytes(Constants.FALSE);
			catalogColumn.writeBytes(Constants.FALSE);
			
			catalogColumn.close();
			
			String[] cur_row_id_value = {"10", Constants.TABLE_CATALOG,"cur_row_id","INT","3",Constants.FALSE,Constants.FALSE};		
			DatabaseHelper.insertInto(Constants.COLUMN_CATALOG,cur_row_id_value,Constants.dirCatalog);			//add current row_id column to davisbase_columns
		}
		catch (Exception e) {
			System.out.println(e);
		}
}

}
