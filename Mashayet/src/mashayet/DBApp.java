package mashayet;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import Exceptions.DBAppException;

public class DBApp {
	private ArrayList<Table> tables = new ArrayList();

	public void init() {
		// this does whatever initialization you would like
	}

	// or leave it empty if there is no code you want to
	// execute at application startup
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		Table createdTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		this.tables.add(createdTable);
	}

	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {
				c.insertSortedTuple(htblColNameValue);

			}

		});
	}

	public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {
				c.updateTuple(strKey,htblColNameValue);

			}

		});
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {
				

			}

		});

	}

	// public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
	// String[] strarrOperators)
	// throws DBAppException {
	//
	// }
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		DBApp app = new DBApp();
		try {
			app.createTable(strTableName, "id", htblColNameType);
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(23498));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(1));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Ypussef Fathi"));
			htblColNameValue.put("gpa", new Integer(1));
//			app.updateTable(strTableName,new Integer(78452), htblColNameValue);
System.out.println("************************");
Table t=app.tables.get(0);
for(int i=0;i<t.getPages().size();i++){
	t.readPage(i);
}
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
