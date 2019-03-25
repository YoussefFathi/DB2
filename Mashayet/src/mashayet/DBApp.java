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
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {
				try {
					c.createBitmapIndex(strColName);
				} catch (DBAppException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		});
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {

				try {
					c.insertSortedTuple(htblColNameValue);

				} catch (DBAppException e) {
					// TODO Auto-generated catch block
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
				//

			}

		});
	}

	public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {
				c.updateTuple(strKey, htblColNameValue);

			}

		});
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		tables.forEach((c) -> {
			if (c.getName().equals(strTableName)) {

				c.deleteTuple(htblColNameValue);
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
			htblColNameValue.put("gpa", new Double(1));
			app.updateTable(strTableName, new Integer(78452), htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(1));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.deleteFromTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.deleteFromTable(strTableName, htblColNameValue);
			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5674567));
			// htblColNameValue.put("name", new String("Dalia Noor"));
			// htblColNameValue.put("gpa", new Double(1.25));
			// htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(5674567));
			// htblColNameValue.put("name", new String("Dalia Noor"));
			// htblColNameValue.put("gpa", new Double(1.25));
			// htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.35));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784352));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784351));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784353));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(7843544));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784));
			htblColNameValue.put("name", new String("Zaky bo2loz Youssef"));
			htblColNameValue.put("gpa", new Integer(7));
			app.updateTable(strTableName, new Integer(7843544), htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784));
			htblColNameValue.put("name", new String("Zaky bo2loz Youssef2"));
			htblColNameValue.put("gpa", new Double(7));
			app.updateTable(strTableName, new Integer(784353), htblColNameValue);

			htblColNameValue.clear();

			htblColNameValue.put("name", new Integer(1));

			app.deleteFromTable(strTableName, htblColNameValue);
			app.createBitmapIndex(strTableName, "name");
			app.createBitmapIndex(strTableName, "gpa");
			app.createBitmapIndex(strTableName, "id");
			 htblColNameValue.clear();
			 htblColNameValue.put("id", new Integer(567456127));
			 htblColNameValue.put("name", new String("Dalaia Noor"));
			 htblColNameValue.put("gpa", new Double(1.25));
			 app.insertIntoTable(strTableName, htblColNameValue);

			System.out.println("************************");
			Table t = app.tables.get(0);
			System.out.println(t.getPages().toString());
			for (int i = 0; i < t.getPages().size(); i++) {
				System.out.println("Page :" + i);
				t.readPage(i);
			}
			boolean found = false;
			int first = 0;
			System.out.println(t.getBitmapPages());
			for (int i = 0; i < t.getBitmapPages().size(); i++) {
				if ((t.getBitmapPages().get(i)).equals("gpa") && !found) {
					first = i;
					found = true;
				} else if (found && !((t.getBitmapPages().get(i)).equals("gpa"))) {
					break;
				}
				System.out.println("BitMapPage :" + (i - first));
				t.readBitmapPage(i - first, "gpa");
			}
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
