package com.demodb.util;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;

public class ConsoleWriter {
	
	public static void write(Object message){
		System.out.println(message);
	}
	
	public static void write(Message message){
		write(message.getMessage());
	}
	
	public static void write(ResultSet resultSet){
		write(resultSet.getRowAffected() + " rows affected");
		write(resultSet.getData());
	}
}
