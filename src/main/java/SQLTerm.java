import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;

public class SQLTerm {
	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue ;
	
	public SQLTerm() {
		
	}
	public SQLTerm(String tableName, String columnName, String operator, Object value) {
		super();
		_strTableName = tableName;
		_strColumnName = columnName;
		_strOperator = operator;
		_objValue = value;
	}

	public String getTableName() {
		return _strTableName;
	}


	public void setTableName(String tableName) {
		_strTableName = tableName;
	}


	public String getColumnName() {
		return _strColumnName;
	}


	public void setColumnName(String columnName) {
		_strColumnName = columnName;
	}


	public String getOperator() {
		return _strOperator;
	}


	public void setOperator(String operator) {
		_strOperator = operator;
	}


	public Object getValue() {
		return _objValue;
	}


	public void setValue(Object value) {
		_objValue = value;
	}
	

}
