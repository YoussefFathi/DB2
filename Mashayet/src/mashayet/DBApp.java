 package mashayet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
//			System.out.println("Serialized data is saved in " + tableName + ".class");
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
			count++;
		}
		System.out.println(temp);
		return t.getVectorResult(temp).iterator();

	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		try{
		DBApp app=new DBApp();
	String strTableName = "Student";
	Hashtable htblColNameType = new Hashtable( );
	htblColNameType.put("id", "java.lang.Integer");
	htblColNameType.put("name", "java.lang.String");
	htblColNameType.put("gpa", "java.lang.Double");
	app.createTable( strTableName, "id", htblColNameType );
	app.createBitmapIndex( strTableName, "gpa" );
	Hashtable htblColNameValue = new Hashtable( );
	htblColNameValue.put("id", new Integer( 2343432 ));
	htblColNameValue.put("name", new String("Ahmed Noor" ) );
	htblColNameValue.put("gpa", new Double( 0.95 ) );
	app.insertIntoTable( strTableName , htblColNameValue );
	htblColNameValue.clear( );
	htblColNameValue.put("id", new Integer( 453455 ));
	htblColNameValue.put("name", new String("Ahmed Noor" ) );
	htblColNameValue.put("gpa", new Double( 0.95 ) );
	app.insertIntoTable( strTableName , htblColNameValue );
	htblColNameValue.clear( );
	htblColNameValue.put("id", new Integer( 5674567 ));
	htblColNameValue.put("name", new String("Dalia Noor" ) );
	htblColNameValue.put("gpa", new Double( 1.25 ) );
	app.insertIntoTable( strTableName , htblColNameValue );
	htblColNameValue.clear( );
	htblColNameValue.put("id", new Integer( 23498 ));
	htblColNameValue.put("name", new String("John Noor" ) );
	htblColNameValue.put("gpa", new Double( 1.5 ) );
	app.insertIntoTable( strTableName , htblColNameValue );
	htblColNameValue.clear( );
	htblColNameValue.put("id", new Integer( 78452 ));
	htblColNameValue.put("name", new String("Zaky Noor" ) );
	htblColNameValue.put("gpa", new Double( 0.88 ) );
	app.printing(app);
	try {
		app.insertIntoTable( strTableName , htblColNameValue );
	} catch (DBAppException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	SQLTerm[] arrSQLTerms;
	arrSQLTerms = new SQLTerm[2];
	arrSQLTerms[0] = new SQLTerm("Student", "gpa", ">", "1.0");
	arrSQLTerms[1] = new SQLTerm("Student", "name", ">", "John Noor");
	String[] strarrOperators = new String[1];
	strarrOperators[0] = "AND";
	Iterator resultSet = app.selectFromTable(arrSQLTerms, strarrOperators);
	while (resultSet.hasNext()) {
		System.out.println(resultSet.next());
	}
		catch(Exception e){
			e.printStackTrace();
		}}

	public void printing(DBApp app) {
		try {
			Table t = app.readTable("Student");
			for (int i = 0; i < t.getPages().size(); i++) {
				System.out.println("Page :" + i);
				t.readPage2(i);
			}
			boolean found = false;
			int first = 0;
			System.out.println(t.getBitmapPages());
			for (int i = 0; i < t.getBitmapPages().size(); i++) {
				if ((t.getBitmapPages().get(i)).equals("gpa") && !found) {
					first = i;
					found = true;
					System.out.println("BitMapPage :" + (i - first));
					t.readBitmapPage2(i - first, "gpa");
				} else if (found && (t.getBitmapPages().get(i)).equals("gpa")) {
					System.out.println("BitMapPage :" + (i - first));
					t.readBitmapPage2(i - first, "gpa");
				} else if (found && !((t.getBitmapPages().get(i)).equals("gpa"))) {

				}
			}

			System.out.println(t.getPages());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
