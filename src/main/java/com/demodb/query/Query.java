/**
 * 
 */
package com.demodb.query;

import com.demodb.dto.Message;
import com.demodb.dto.ResultSet;

/**
 * @author Prashant  Yadav
 *
 */
public interface Query {
	public ResultSet execute();
	public Message isValid();
	public void displayContent(ResultSet resultSet);
}
