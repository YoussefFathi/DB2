package mashayet;


import java.util.Hashtable;
import java.util.Iterator;

import Exceptions.DBAppException;

public class DBApp {
	public void init( ) {
		// this does whatever initialization you would like
	}
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void createTable(String strTableName,
	 String strClusteringKeyColumn,
	Hashtable<String,String> htblColNameType ) 
	 throws DBAppException
	 {
		 
	 }
	public void createBitmapIndex(String strTableName,
	 String strColName) throws DBAppException{
		
	}
	public void insertIntoTable(String strTableName,
	 Hashtable<String,Object> htblColNameValue)
	 throws DBAppException
	 {
		
	 }
	public void updateTable(String strTableName,
	 String strKey,
	Hashtable<String,Object> htblColNameValue )
	throws DBAppException
	{
		
	}
	public void deleteFromTable(String strTableName,
	 Hashtable<String,Object> htblColNameValue)
	 throws DBAppException
	 {
		
	 }
//	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
//	 String[] strarrOperators)
//	throws DBAppException {
//		
//	}
	public static void main(String[] args) {

	}

}
