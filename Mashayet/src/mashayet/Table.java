package mashayet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;
import java.util.function.Consumer;
import java.util.Properties;
import Exceptions.DBAppException;

public class Table implements Serializable {

	// private static final long serialVersionUID = 1L;
	transient private ArrayList<Integer> pages = new ArrayList();
	transient private ArrayList<String> BitmapPages = new ArrayList();
	private String tableName = "";
	private int maxRows;

	public ArrayList<String> getBitmapPages() {
		return BitmapPages;
	}

	public void setBitmapPages(ArrayList<String> bitmapPages) {
		BitmapPages = bitmapPages;
	}

	private int noRows = 0;
	private String tableKey = "";
	private int attrNo = 0;
	private ArrayList columnNames = new ArrayList();
	private Properties properties = new Properties();
	private ArrayList <String> bitmappedCols=new ArrayList();

	public Table(String tableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) {
		this.tableName = tableName;
		try {
			this.addToMeta(strClusteringKeyColumn, htblColNameType);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Page first = new Page();
		pages.add(0);
		this.writePage(first, 0);
		addToProps();
		maxRows = Integer.parseInt(properties.getProperty("maxRows"));
	}

	public void addToProps() {
		try {
			FileWriter file = new FileWriter(new File("./config/DBApp.config"));
			properties.setProperty("maxRows", "2");
			properties.setProperty("tableName", tableName);
			properties.store(file, null);
			file.flush();
			file.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addToMeta(String key, Hashtable<String, String> table) throws IOException {
		FileWriter writer = new FileWriter(new File("./data/metaData.csv"));
		try {
			writer.append("Table Name, Column Name, Column Type, Key,Indexed ");
			writer.append('\n');
			table.forEach((name, type) -> {
				try {
					columnNames.add(name);
					attrNo++;
					writer.append(this.tableName);
					writer.append(',');
					writer.append(name);
					writer.append(',');
					writer.append(type);
					writer.append(',');
					if (key.equals(name)) {
						tableKey = key;
						writer.append("True");
					} else {
						writer.append("False");
					}
					writer.append(',');
					writer.append("False");
					writer.append('\n');

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			System.out.println("Meta was created Successfully");
		} finally {
			writer.flush();
			writer.close();
		}
	}

	public String getName() {
		return tableName;

	}

	public ArrayList getArrayFromHash(Hashtable<String, Object> hash) {
		ArrayList attrs = new ArrayList();
		hash.forEach((name, value) -> {
			attrs.add(columnNames.indexOf(name), value);
		});
		return attrs;
	}

	public void updateTuple(Object key, Hashtable<String, Object> htblColNameValue) {
		for (int i = 0; i < pages.size(); i++) {
			Page tempPage = readPage(pages.get(i));
			Vector tuples = tempPage.readTuples();
			for (int j = 0; j < tuples.size(); j++) {
				if (((Tuple) tuples.get(j)).getAttributes().contains(key)) {
					ArrayList attrs = getArrayFromHash(htblColNameValue);
					Tuple removed = (Tuple) tuples.remove(j);
					// Tuple temp = new
					// Tuple(removed.getAttributes(),removed.getKeyIndex(),removed.getColName());
					// removed.setAttributes(attrs);
					this.writePage(tempPage, i);
					try {
						insertSortedTuple(htblColNameValue);
					} catch (DBAppException e) {
						System.out.println(e.getMessage());
						tuples.add(j, removed);
						this.writePage(tempPage, i);

					}
					readPage(i);
					return;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public boolean findKey(String key, Page page) {
		boolean found = false;
		for (int i = 0; i < page.readTuples().size(); i++) {
			if (((Tuple) page.readTuples().get(i)).getAttributes().contains(key)) {
				return true;
			}
		}
		return false;

	}

	public void insertTuple(Hashtable<String, Object> htblColNameValue) {
		int pageNo = 0;
		Page currentPage = null;
		if (noRows == maxRows) {
			pageNo = pages.size();
			currentPage = new Page();
			pages.add(pageNo);
			noRows = 0;
		} else {
			pageNo = pages.size() - 1;
			currentPage = readPage(pageNo);
		}
		ArrayList attrs = new ArrayList(attrNo);
		Set<String> names = htblColNameValue.keySet();
		int key = 0;
		for (String name : names) {
			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {
				attrs.add(value);
				if (name.equals(tableKey)) {
					key = attrs.size();
				}
			} else {
				System.out.println("Invalid Input for" + name + " " + value);
				return;
			}

		}

		currentPage.addTuple(new Tuple(attrs, key, null));

		writePage(currentPage, pageNo);
		readPage(pageNo);
		noRows++;

	}
	// public Class<?> getType(String name) {
	// switch (name) {
	// case "java.lang.Integer":return int.class;
	// case "java.lang.String":return String.class;
	// case "java.lang.Double":return double.class;
	// case "java.lang.Boolean":return boolean.class;
	// case "java.util.Date":return Date.class;
	// default:return null;
	// }
	// }

	public boolean checkType(String name, Object value) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("./data/metaData.csv")));
			reader.readLine();
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if ((name.equals(parts[1])) && (value.getClass().getName().equals(parts[2]))) {
					return true;
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public void writePage(Page page, int indicator) {

		try {
			FileOutputStream fileOut = new FileOutputStream("./data/" + tableName + " P" + indicator + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + tableName + " P" + indicator + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeBitmapPage(BitMapPage page, int indicator, String colName) {

		try {
			FileOutputStream fileOut = new FileOutputStream(
					"./data/" + tableName + "B " + colName + indicator + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + tableName + "B " + colName + indicator + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public BitMapPage readBitmapPage(int indicator, String colName) {

		try {
			FileInputStream fileIn = new FileInputStream("./data/" + tableName + "B " + colName + indicator + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			BitMapPage e = (BitMapPage) in.readObject();
			int i = 0;
			e.readTuples().forEach((b) -> {

				System.out.println(((BitmapObject) b).getColValue() + ": " + ((BitmapObject) b).getBitmap());

			});

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

	public Page readPage(int indicator) {

		try {
			FileInputStream fileIn = new FileInputStream("./data/" + tableName + " P" + indicator + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page e = (Page) in.readObject();
			int i = 0;
			e.readTuples().forEach((b) -> {
				System.out.print("TUPLE :");
				System.out.println(((Tuple) b).getAttributes());

			});

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

	public void deleteTuple(Hashtable<String, Object> htblColNameValue) {
		Page currentPage = null;
		ArrayList attrs = new ArrayList(attrNo);

		ArrayList colNames = new ArrayList();
		Set<String> names = htblColNameValue.keySet();
		int key = -1;
		for (String name : names) {

			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {

				attrs.add(value);
				colNames.add(name);
				if (name.equals(tableKey)) {
					key = attrs.size() - 1;
				}
			} else {
				System.out.println("Invalid Input for" + name + " " + value);

				return;
			}

		}

		Tuple tupleToDelete = new Tuple(attrs, key, colNames);
		for (int i = 0; i < pages.size(); i++) {
			currentPage = readPage(i);
			Vector<Tuple> tempVector = currentPage.readTuples();
			for (int j = 0; j < tempVector.size(); j++) {
				if (tempVector.get(j).compareTo(tupleToDelete) == 0) {
					tempVector.remove(j--);
					if (tempVector.size() == 0 && i != pages.size() - 1) {
						shiftPagesUp(i);
					} else if (!(tempVector.size() == 0) && i != pages.size() - 1) {
						writePage(currentPage, i);
					} else if (tempVector.size() == 0 && i == pages.size() - 1) {
						removePage(i);
					}
				}
			}
		}
	}

	public void removePage(int pageNo) {
		pages.remove(pageNo);
		File toBeDeleted = new File(tableName + " P" + pageNo + ".class");

		if (toBeDeleted.delete()) {
			System.out.println("File" + pageNo + "Deleted");
		}
	}

	public void shiftPagesUp(int startPage) {
		pages.remove(startPage);
		for (int i = startPage; i < pages.size(); i++) {
			pages.set(i, i);
			Page currentPage = this.readPage(i + 1);
			this.writePage(currentPage, i);
		}
		File toBeDeleted = new File(tableName + " P" + pages.size() + ".class");
		if (toBeDeleted.delete()) {
			System.out.println("File" + pages.size() + "Deleted");
		}
	}

	public void insertSortedBitmap(BitmapObject tupleToInsert, String colName) throws DBAppException {
		BitMapPage currentPage = null;
		int i = 0;
		boolean first = false;
		boolean start = false;
		int[] pageTupleNo ;
		int count = -1;
		int startInPages = 0;
		ArrayList<String> temp = new ArrayList<String>();

		for (int k = 0; k < BitmapPages.size(); k++) {
			if (BitmapPages.get(k).equals(colName) && !start) {
				startInPages = k;
				start = true;
				temp.add(colName);
			} else if (!BitmapPages.get(k).equals(colName) && start) {
				break;
			} else if (BitmapPages.get(k).equals(colName)) {
				temp.add(colName);
			}
		}
		System.out.println(BitmapPages);
		System.out.println(temp + "TEMPPP");
		for (i = 0; i < temp.size() - 1; i++) {

			currentPage = readBitmapPage(i, colName);
			Vector<BitmapObject> tempVector = currentPage.readTuples();
			for (int j = 0; j < tempVector.size(); j++) {
				System.out.println(tupleToInsert);
				System.out.println(tempVector.get(j));
				if (tempVector.get(j).compareTo(tupleToInsert) == 2
						|| tempVector.get(j).compareTo(tupleToInsert) == 0) {
					throw new DBAppException("Duplicate Insertion");
				}
				if (tempVector.get(j).compareTo(tupleToInsert) > 0) {
					if (j == 0 && i > 0) {
						BitMapPage previousPage = readBitmapPage(i - 1, colName);
						previousPage.addTuple(tupleToInsert);
						previousPage.sort();
						if (tempVector.size() > maxRows) {
							BitmapObject overFlowTuple = tempVector.remove(maxRows);
							writeBitmapPage(previousPage, i - 1, colName);
							shiftingPages(overFlowTuple, i - 1, colName, temp,startInPages);

						} else {
							writeBitmapPage(previousPage, i - 1, colName);

						}
						return;
					} else {
						currentPage.addTuple(tupleToInsert);
						currentPage.sort();
						
						if (tempVector.size() > maxRows) {
							BitmapObject overFlowTuple = tempVector.remove(maxRows);
							writeBitmapPage(currentPage, i, colName);
							shiftingPages(overFlowTuple, ++i, colName, temp,startInPages);

						} else {
							writeBitmapPage(currentPage, i, colName);

						}
					}
					return;
				}

			}

		}

		currentPage = readBitmapPage(temp.size() - 1, colName);
		Vector<BitmapObject> tempVector = currentPage.readTuples();
		for (int j = 0; j < tempVector.size(); j++) {
			if (tempVector.get(j).compareTo(tupleToInsert) == 2 || tempVector.get(j).compareTo(tupleToInsert) == 0) {
				throw new DBAppException("Duplicate Insertion");
			}
		}
		if (tempVector.size() == maxRows) {
			currentPage.addTuple(tupleToInsert);
			currentPage.sort();
			BitmapObject overFlow = tempVector.remove(maxRows);
			writeBitmapPage(currentPage, temp.size() - 1, colName);
			currentPage = new BitMapPage();

			BitmapPages.add(startInPages + temp.size(), colName);
			temp.add(colName);
			currentPage.addTuple(overFlow);
			writeBitmapPage(currentPage, temp.size() - 1, colName);

		} else {
			if (currentPage.readTuples().size() > 0) {
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				writeBitmapPage(currentPage, temp.size() - 1, colName);

			} else {
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				int num = 0;
				writeBitmapPage(currentPage, temp.size() - 1, colName);
			}
		}

	}

	public void insertSortedTuple(Hashtable<String, Object> htblColNameValue) throws DBAppException {
		int pageNo = 0;
		ArrayList attrs = new ArrayList(attrNo);
		int countRows=0;
		ArrayList colNames = new ArrayList();
		Set<String> names = htblColNameValue.keySet();
		int key = -1;
		for (String name : names) {

			Object value = htblColNameValue.get(name);
			// System.out.println(name +value);
			if (checkType(name, value)) {

				attrs.add(value);
				colNames.add(name);
				if (name.equals(tableKey)) {
					key = attrs.size() - 1;
				}
			} else {
				throw new DBAppException("Invalid Input " + name + " , " + value);
			}

		}

		Tuple tupleToInsert = new Tuple(attrs, key, colNames);

		Page currentPage = null;

		for (int i = 0; i < pages.size() - 1; i++) {
			currentPage = readPage(i);
			Vector<Tuple> tempVector = currentPage.readTuples();
			for (int j = 0; j < tempVector.size(); j++) {
				System.out.println(tupleToInsert);
				System.out.println(tempVector.get(j));
				if (tempVector.get(j).compareTo(tupleToInsert) == 2
						|| tempVector.get(j).compareTo(tupleToInsert) == 0) {
					throw new DBAppException("Duplicate Insertion");
				}
				if (tempVector.get(j).compareTo(tupleToInsert) > 0) {
					bitmapHandleInsert(tupleToInsert,countRows);
					if (j == 0 && i > 0) {
						Page previousPage = readPage(i - 1);
						previousPage.addTuple(tupleToInsert);
						previousPage.sort();
						if (tempVector.size() > maxRows) {
							Tuple overFlowTuple = tempVector.remove(maxRows);
							writePage(previousPage, i - 1);
							shiftingPages(overFlowTuple, i - 1);

						} else {
							writePage(previousPage, i - 1);

						}
						return;
					} else {
						currentPage.addTuple(tupleToInsert);
						currentPage.sort();
						if (tempVector.size() > maxRows) {
							Tuple overFlowTuple = tempVector.remove(maxRows);
							writePage(currentPage, i);
							shiftingPages(overFlowTuple, ++i);

						} else {
							writePage(currentPage, i);

						}
					}
					return;
				}
				countRows++;
			}

		}

		currentPage = readPage(pages.size() - 1);
		Vector<Tuple> tempVector = currentPage.readTuples();
		for (int j = 0; j < tempVector.size(); j++) {
			if (tempVector.get(j).compareTo(tupleToInsert) == 2 || tempVector.get(j).compareTo(tupleToInsert) == 0) {
				throw new DBAppException("Duplicate Insertion");
			}
		}
		if (tempVector.size() == maxRows) {
			bitmapHandleInsert(tupleToInsert,countRows);
			currentPage.addTuple(tupleToInsert);
			currentPage.sort();
			Tuple overFlow = tempVector.remove(maxRows);
			writePage(currentPage, pages.size() - 1);
			currentPage = new Page();
			pages.add(pages.size());
			currentPage.addTuple(overFlow);
			writePage(currentPage, pages.size() - 1);

		} else {
			if (currentPage.readTuples().size() > 0) {
				bitmapHandleInsert(tupleToInsert,countRows);
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				writePage(currentPage, pages.size() - 1);

			} else {
				bitmapHandleInsert(tupleToInsert,countRows);
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				int num = 0;
				writePage(currentPage, pages.size() - 1);
			}
		}
	}

	private void bitmapHandleInsert(Tuple tupleToInsert, int countRows) {
		boolean found=false;
		int pageNo=0;
		int index=-1;
		for(int i=0;i<bitmappedCols.size();i++){
			found=false;
			for(int k=0;k<tupleToInsert.getColName().size();k++){
				if(tupleToInsert.getColName().get(k).equals(bitmappedCols.get(i))){
					index=k;
				}
			}
			for(int j=0;j<BitmapPages.size();j++){
				if(BitmapPages.get(j).equals(bitmappedCols.get(i))){
					BitMapPage bp=readBitmapPage(pageNo,bitmappedCols.get(i));
					Vector <BitmapObject> vec=bp.readTuples();
					for(int k=0;k<vec.size();k++){
						if(tupleToInsert.getAttributes().get(index).equals(vec.get(k))){
							found=true;
							 String b = vec.get(k).getBitmap();
							 StringBuilder str = new StringBuilder(b);
							 str.insert(countRows, '1');
							 vec.get(k).setBitmap(str+"");
							}
						else{
							 String b = vec.get(k).getBitmap();
							 StringBuilder str = new StringBuilder(b);
							 str.insert(countRows, '0');
							 vec.get(k).setBitmap(str+"");
						}
					}
					this.writeBitmapPage(bp,pageNo,bitmappedCols.get(i));
					pageNo++;
					}
			}
			pageNo=0;
			if(!found){
				for (int n = 0; i < pages.size(); n++) { // loop over all pages
					Vector currentTuples = readPage(n).readTuples();
					for (int j = 0; j < currentTuples.size(); j++) { // loop over all tuples per page
						Tuple tuple = ((Tuple) currentTuples.get(j));
						int colIndex = tuple.getColName().indexOf(bitmappedCols.get(i));
						BitmapObject newObj=new BitmapObject(tupleToInsert.getAttributes().get(index),"");
						Object colValue = tuple.getAttributes().get(colIndex);
							if (tupleToInsert.getAttributes().get(index).equals(colValue)) {
								newObj.setBitmap(newObj.getBitmap() + "1");
							} else {
								newObj.setBitmap(newObj.getBitmap() + "0");
							}
							try {
								System.out.println(newObj.getBitmap());
								System.out.println("HI");
								insertSortedBitmap(newObj, bitmappedCols.get(i));
							} catch (DBAppException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}

				}
			}
		}
	}

	public void createBitmapIndex(String strColName) throws DBAppException {
		ArrayList<BitmapObject> uniqueValues = new ArrayList<BitmapObject>();
		bitmappedCols.add(strColName);
		// Retrieved all unique values in column needed
		for (int i = 0; i < pages.size(); i++) {
			Vector currentTuples = readPage(i).readTuples();
			for (int j = 0; j < currentTuples.size(); j++) {
				Tuple tuple = ((Tuple) currentTuples.get(j));
				int colIndex = tuple.getColName().indexOf(strColName);
				boolean found = false;
				for (int k = 0; k < uniqueValues.size(); k++) {
					if (uniqueValues.get(k).getColValue().equals(tuple.getAttributes().get(colIndex))) {
						found = true;
						break;
					}
				}
				if (!found) {
					BitmapObject bitmap = new BitmapObject(tuple.getAttributes().get(colIndex), "");
					uniqueValues.add(bitmap);
				}
			}
		}
		for (int i = 0; i < pages.size(); i++) { // loop over all pages
			Vector currentTuples = readPage(i).readTuples();
			for (int j = 0; j < currentTuples.size(); j++) { // loop over all tuples per page
				Tuple tuple = ((Tuple) currentTuples.get(j));
				int colIndex = tuple.getColName().indexOf(strColName);
				Object colValue = tuple.getAttributes().get(colIndex);
				for (int k = 0; k < uniqueValues.size(); k++) { // loop over all distinct elements
					if (uniqueValues.get(k).getColValue().equals(colValue)) {
						uniqueValues.get(k).setBitmap(uniqueValues.get(k).getBitmap() + "1");
					} else {
						uniqueValues.get(k).setBitmap(uniqueValues.get(k).getBitmap() + "0");
					}
				}
			}

		}
		System.out.println(uniqueValues);
		BitMapPage page = new BitMapPage();
		BitmapPages.add(strColName);
		this.writeBitmapPage(page, 0, strColName);

		for (int i = 0; i < uniqueValues.size(); i++) {
			insertSortedBitmap(uniqueValues.get(i), strColName);
		}
		for (int i = 0; i < uniqueValues.size(); i++) {
			System.out.println(uniqueValues.get(i).getColValue() + " : " + uniqueValues.get(i).getBitmap());
		}

	}

	public ArrayList<Integer> getPages() {
		return pages;
	}

	public void shiftingPages(Tuple overFlowTuple, int index) {
		if (index >= pages.size()) {
			int pageNo = pages.size();
			Page currentPage = new Page();
			pages.add(pageNo);
			currentPage.addTuple(overFlowTuple);
			writePage(currentPage, index);
		} else {
			Page currentPage = readPage(index);
			if (currentPage.readTuples().size() < maxRows) {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				writePage(currentPage, index);
			} else {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				Tuple newOverFlow = (Tuple) currentPage.readTuples().remove(maxRows);
				writePage(currentPage, index);
				shiftingPages(newOverFlow, ++index);
			}

		}
	}

	public void shiftingPages(BitmapObject overFlowTuple, int index, String colName, ArrayList<String> temp,int startIndex) {
		System.out.println(temp + "In Shift");
		if (index >= temp.size()) {
			int pageNo = temp.size();
			BitMapPage currentPage = new BitMapPage();
			BitmapPages.add(temp.size()+startIndex, colName);
			temp.add(colName);
			currentPage.addTuple(overFlowTuple);
			writeBitmapPage(currentPage, index, colName);
		} else {
			BitMapPage currentPage = readBitmapPage(index, colName);
			if (currentPage.readTuples().size() < maxRows) {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				writeBitmapPage(currentPage, index, colName);
			} else {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				BitmapObject newOverFlow = (BitmapObject) currentPage.readTuples().remove(maxRows);
				writeBitmapPage(currentPage, index, colName);
				shiftingPages(newOverFlow, ++index, colName, temp,startIndex);
			}

		}

	}

	public static void main(String[] args) {
	}

}
