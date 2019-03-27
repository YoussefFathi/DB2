package mashayet;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import Exceptions.DBAppException;

public class DBApp {
	private ArrayList<String> tables = new ArrayList<String>();

	public void init() {
		// this does whatever initialization you would like
	}

	// or leave it empty if there is no code you want to
	// execute at application startup
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		Table createdTable = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
		this.writeTable(createdTable, strTableName);
		this.tables.add(strTableName);
	}

	public void createBitmapIndex(String strTableName, String strColName) throws DBAppException {
		Table c = readTable(strTableName);
		c.createBitmapIndex(strColName);
		this.writeTable(c, strTableName);

	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Table c = readTable(strTableName);
		try {
			c.insertSortedTuple(htblColNameValue);
			this.writeTable(c, strTableName);
		} catch (DBAppException e) {
			System.out.println(e.getMessage());
		}

	}

	public void updateTable(String strTableName, Object strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Table c = readTable(strTableName);
		c.updateTuple(strKey, htblColNameValue);
		this.writeTable(c, strTableName);

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Table c = readTable(strTableName);
		c.deleteTuple(htblColNameValue);
		this.writeTable(c, strTableName);

	}

	public void writeTable(Table table, String tableName) {

		try {
			FileOutputStream fileOut = new FileOutputStream("./data/" + tableName + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(table);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + tableName + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Table readTable(String tableName) {

		try {
			FileInputStream fileIn = new FileInputStream("./data/" + tableName + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Table e = (Table) in.readObject();
			// int i = 0;
			// e.readTuples().forEach((b) -> {
			// System.out.print("TUPLE :");
			// System.out.println(((Tuple) b).getAttributes());
			//
			// });

			in.close();
			fileIn.close();
			return e;
		} catch (IOException i) {
			i.printStackTrace();
			return null;

		} catch (ClassNotFoundException c) {
			System.out.println("Page class not found");
			c.printStackTrace();
			return null;
		}
	}

	public static String andBitmap(String bm1, String bm2) {
		String result = "";
		// String bm1=this.getBitmap();
		// String bm2=bo.getBitmap();
		for (int i = 0; i < bm1.length(); i++) {
			if (bm1.charAt(i) == '0' || bm2.charAt(i) == '0') {
				result = result + "0";
			} else
				result = result + "1";
		}
		return result;
	}

	public static String xorBitmap(String bm1, String bm2) {
		String result = "";
		// String bm1=this.getBitmap();
		// String bm2=bo.getBitmap();
		for (int i = 0; i < bm1.length(); i++) {
			if (bm1.charAt(i) == '0' || bm2.charAt(i) == '0') {
				result = result + "0";
			} else if (bm1.charAt(i) == '1' || bm2.charAt(i) == '1') {
				result = result + "0";
			} else
				result = result + "1";
		}
		return result;
	}

	public static String orBitmap(String bm1, String bm2) {
		String result = "";
		// String bm1=this.getBitmap();
		// String bm2=bo.getBitmap();
		for (int i = 0; i < bm1.length(); i++) {
			if (bm1.charAt(i) == '1' || bm2.charAt(i) == '1') {
				result = result + "1";
			} else
				result = result + "0";
		}
		return result;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
		Queue<String> resultSet = new LinkedList<String>();
		Table t = null;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			String colName = arrSQLTerms[i]._strColumnName;
			t = readTable(arrSQLTerms[i]._strTableName);
			if (t.isIndexed(colName)) {
				resultSet.add(t.queryIndexed(arrSQLTerms[i]));
			} else {
				resultSet.add(t.queryNormal(arrSQLTerms[i]));
			}

		}
		System.out.println(resultSet);
		String temp = resultSet.poll();
		int count = 0;
		while (resultSet.size() >= 1 && count < strarrOperators.length) {
			switch (strarrOperators[count]) {
			case "AND":
				temp = andBitmap(temp, resultSet.poll());
				break;

			case "OR":
				temp = orBitmap(temp, resultSet.poll());
				break;
			case "XOR":
				temp = xorBitmap(temp, resultSet.poll());
				break;
			}
		}
		System.out.println(temp);
		return t.getVectorResult(temp).iterator();

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		String strTableName2 = "Admin";
		Hashtable htblColNameType2 = new Hashtable();
		htblColNameType2.put("id2", "java.lang.Integer");
		htblColNameType2.put("name2", "java.lang.String");
		htblColNameType2.put("gpa2", "java.lang.Double");
		DBApp app = new DBApp();
		try {
			app.createTable(strTableName2, "name2", htblColNameType2);
			Hashtable htblColNameValue2 = new Hashtable();
			htblColNameValue2.put("id2", new Integer(2343432));
			htblColNameValue2.put("name2", new String("Youssef Fathi"));
			htblColNameValue2.put("gpa2", new Double(0.95));
			app.insertIntoTable(strTableName2, htblColNameValue2);
			app.createTable(strTableName, "id", htblColNameType);
			Hashtable htblColNameValue = new Hashtable();
			// app.createBitmapIndex(strTableName, "name");
			// app.createBitmapIndex(strTableName, "gpa");
			app.createBitmapIndex(strTableName, "id");
			htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.insertIntoTable(strTableName, htblColNameValue);
			// app.createBitmapIndex(strTableName, "name");
			// app.createBitmapIndex(strTableName, "gpa");
			// app.createBitmapIndex(strTableName, "id");
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(5674567));
			htblColNameValue.put("name", new String("Dalia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(23498));
			htblColNameValue.put("name", new String("John Noor"));
			htblColNameValue.put("gpa", new Double(1.5));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(1));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Ypussef Fathi"));
			htblColNameValue.put("gpa", new Double(1));
			// app.updateTable(strTableName, new Integer(78452), htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(78452));
			htblColNameValue.put("name", new String("Zaky Noor"));
			htblColNameValue.put("gpa", new Double(1));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(2343432));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			// app.deleteFromTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(453455));
			htblColNameValue.put("name", new String("Ahmed Noor"));
			htblColNameValue.put("gpa", new Double(0.95));
			// app.deleteFromTable(strTableName, htblColNameValue);
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
			// app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784352));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784351));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784353));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(7843544));
			htblColNameValue.put("name", new String("Zaky bo2loz Noor"));
			htblColNameValue.put("gpa", new Double(11));
			app.insertIntoTable(strTableName, htblColNameValue);
			app.printing(app);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784));
			htblColNameValue.put("name", new String("Zaky bo2loz Youssef"));
			htblColNameValue.put("gpa", new Integer(7));
			// app.updateTable(strTableName, new Integer(7843544), htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(784));
			htblColNameValue.put("name", new String("Zaky bo2loz Youssef2"));
			htblColNameValue.put("gpa", new Double(7));
			// app.updateTable(strTableName, new Integer(784353), htblColNameValue);

			htblColNameValue.clear();

			// htblColNameValue.put("name", new Integer(1));

			// app.deleteFromTable(strTableName, htblColNameValue);

			// app.createBitmapIndex(strTableName, "name");
			// app.createBitmapIndex(strTableName, "gpa");
			// app.createBitmapIndex(strTableName, "id");

			htblColNameValue.clear();
			htblColNameValue.put("id", new Integer(567456127));
			htblColNameValue.put("name", new String("Dalaia Noor"));
			htblColNameValue.put("gpa", new Double(1.25));
			app.insertIntoTable(strTableName, htblColNameValue);
			htblColNameValue.clear();
			// htblColNameValue.put("id", new Integer(784352));
			// htblColNameValue.put("name", new String("Dalaia Noor"));
			htblColNameValue.put("gpa", new Double(11.0));
			app.deleteFromTable(strTableName, htblColNameValue);
			app.printing(app);

			System.out.println("************************");
			SQLTerm[] arrSQLTerms;
			arrSQLTerms = new SQLTerm[2];
			arrSQLTerms[0] = new SQLTerm("Student", "gpa", ">", "1.0");
			arrSQLTerms[1] = new SQLTerm("Student", "name", "=", "John Noor");
			// arrSQLTerms[0]._strTableName = "Student";
			// arrSQLTerms[0]._strColumnName= "name";
			// arrSQLTerms[0]._strOperator = "=";
			// arrSQLTerms[0]._objValue = "John Noor";
			String[] strarrOperators = new String[1];
			strarrOperators[0] = "AND";
			Iterator resultSet = app.selectFromTable(arrSQLTerms, strarrOperators);
			while(resultSet.hasNext()) {
				System.out.println(resultSet.next());
			}
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void printing(DBApp app) {
		Table t = app.readTable("Student");
		for (int i = 0; i < t.getPages().size(); i++) {
			System.out.println("Page :" + i);
			t.readPage(i);
		}
		boolean found = false;
		int first = 0;
		System.out.println(t.getBitmapPages());
		for (int i = 0; i < t.getBitmapPages().size(); i++) {
			if ((t.getBitmapPages().get(i)).equals("id") && !found) {
				first = i;
				found = true;
			} else if (found && !((t.getBitmapPages().get(i)).equals("id"))) {
				break;
			}
			System.out.println("BitMapPage :" + (i - first));
			t.readBitmapPage(i - first, "id");
		}
		System.out.println(t.getPages());
	}
}
