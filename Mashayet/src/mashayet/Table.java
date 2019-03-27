package mashayet;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
	private ArrayList<Integer> pages = new ArrayList();
	private ArrayList<String> BitmapPages = new ArrayList();
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
	private ArrayList<String> bitmappedCols = new ArrayList();

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
		FileWriter writer = new FileWriter(new File("./data/metaData.csv"), true);
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
		System.out.println(columnNames + "COL NAMES");
		ArrayList attrs = new ArrayList();
		hash.forEach((name, value) -> {
			attrs.add(columnNames.indexOf(name), value);
		});
		return attrs;
	}

	public void updateTuple(Object key, Hashtable<String, Object> htblColNameValue) {
		int countRows = 0;
		for (int i = 0; i < pages.size(); i++) {
			Page tempPage = readPage(pages.get(i));
			Vector tuples = tempPage.readTuples();
			for (int j = 0; j < tuples.size(); j++) {
				if (((Tuple) tuples.get(j)).getAttributes().contains(key)) {
					ArrayList attrs = getArrayFromHash(htblColNameValue);
					Tuple removed = (Tuple) tuples.remove(j);
					handleDelete(removed,countRows);
					countRows--;
					// Tuple temp = new
					// Tuple(removed.getAttributes(),removed.getKeyIndex(),removed.getColName());
					// removed.setAttributes(attrs);
					this.writePage(tempPage, i);
					try {
						insertSortedTuple(htblColNameValue);
						ArrayList attrsTemp = new ArrayList(attrNo);
						
						ArrayList colNames = new ArrayList();
						Set<String> names = htblColNameValue.keySet();
						int keyTemp = -1;
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

						Tuple tupleToInsert = new Tuple(attrs, keyTemp, colNames);
						bitmapHandleInsert(tupleToInsert,countRows);
					} catch (DBAppException e) {
						System.out.println(e.getMessage());
						tuples.add(j, removed);
						bitmapHandleInsert(removed,countRows);
						this.writePage(tempPage, i);

					}
					readPage(i);
					return;
				}
				countRows++;
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

	public void updateMeta(String colName) {
		try {
			File file = new File("./data/metaData.csv");
			BufferedReader reader = new BufferedReader(new FileReader(new File("./data/metaData.csv")));

			ArrayList<String> lines = new ArrayList();
			// String first = reader.readLine();
			lines.add(reader.readLine());
			String line = "";
			while ((line = reader.readLine()) != null) {

				String newLine = "";
				String[] parts = line.split(",");
				if (parts[0].equals(tableName)) {

					if ((colName.equals(parts[1]))) {
						parts[4] = "TRUE";
						for (int i = 0; i < parts.length; i++) {
							if (i != parts.length - 1)
								newLine = newLine + parts[i] + ",";
							else
								newLine = newLine + parts[i];
						}

						lines.add(newLine);

					} else {
						lines.add(line);
					}
				} else {
					lines.add(line);
				}
			}
			reader.close();
			FileWriter writer1 = new FileWriter(new File("./data/metaData.csv"));
			writer1.write("");
			FileWriter writer = new FileWriter(new File("./data/metaData.csv"), true);
			for (int i = 0; i < lines.size(); i++) {
				writer.append(lines.get(i) + "");
				writer.append('\n');
			}

			writer.flush();
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
		int countRows = 0;
		Tuple tupleToDelete = new Tuple(attrs, key, colNames);
		for (int i = 0; i < pages.size(); i++) {
			currentPage = readPage(i);
			Vector<Tuple> tempVector = currentPage.readTuples();
			for (int j = 0; j < tempVector.size(); j++) {
				if (tempVector.get(j).compareTo(tupleToDelete) == 0) {
					tempVector.remove(j--);
					if (tempVector.size() == 0 && i != pages.size() - 1) {
						shiftPagesUp(i);
						handleDelete(tupleToDelete, countRows);
						countRows--;
					} else if (!(tempVector.size() == 0) && i != pages.size() - 1) {
						writePage(currentPage, i);
						handleDelete(tupleToDelete, countRows);
						countRows--;
						pages.set(i, currentPage.readTuples().size());
					} else if (tempVector.size() == 0 && i == pages.size() - 1) {
						removePage(i);
						handleDelete(tupleToDelete, countRows);
						countRows--;
					} else {
						writePage(currentPage, i);
						pages.set(i, currentPage.readTuples().size());
						handleDelete(tupleToDelete, countRows);
						countRows--;
					}

				}
				countRows++;
			}

		}

	}

	public void deleteBitMapObject(BitmapObject bo, String col) {
		BitMapPage currentPage = null;
		int pages = 0;
		;
		int key = -1;
		for (int i = 0; i < BitmapPages.size(); i++) {
			currentPage = readBitmapPage(i, col);
			Vector<BitmapObject> tempVector = currentPage.readTuples();
			if (BitmapPages.get(i).equals(col)) {
				pages++;
			}
			for (int j = 0; j < tempVector.size(); j++) {
				if (tempVector.get(j).compareTo(bo) == 0) {
					tempVector.remove(j--);
					if (tempVector.size() == 0 && i != BitmapPages.size() - 1) {
						shiftPagesUp(i);
					} else if (!(tempVector.size() == 0) && i != BitmapPages.size() - 1) {
						writeBitmapPage(currentPage, pages, col);
					} else if (tempVector.size() == 0 && i == BitmapPages.size() - 1) {
						bitMapremovePage(pages, col);

					}else {
						writeBitmapPage(currentPage, pages, col);
					}
				}
			}
		}
	}

	private void handleDelete(Tuple tupleToDelete, int countRows) {
		int index = -1;
		int pageNo = 0;
		for (int i = 0; i < bitmappedCols.size(); i++) {
			for (int k = 0; k < tupleToDelete.getColName().size(); k++) {
				if (tupleToDelete.getColName().get(k).equals(bitmappedCols.get(i))) {
					index = k;
				}
			}

			for (int j = 0; j < BitmapPages.size(); j++) {
				if (BitmapPages.get(j).equals(bitmappedCols.get(i))) {
					BitMapPage bp = readBitmapPage(pageNo, bitmappedCols.get(i));
					Vector<BitmapObject> vec = bp.readTuples();
					boolean first = true;
					for (int k = 0; k < vec.size(); k++) {

						String b = vec.get(k).getBitmap();
						StringBuilder str = new StringBuilder(b);
						System.out.println("Before=  " + str);
						str.deleteCharAt(countRows);
						System.out.println("After= " + str);
						vec.get(k).setBitmap(str + "");
						if (!vec.get(k).getBitmap().contains("1")) {
							deleteBitMapObject(vec.get(k), bitmappedCols.get(i));
						}

					}
					this.writeBitmapPage(bp, pageNo, bitmappedCols.get(i));
					pageNo++;
				}
			}
			pageNo = 0;
		}
	}

	public void bitMapremovePage(int pageNo, String col) {
		int count = 0;
		int i = 0;
		for (; i < BitmapPages.size(); i++) {
			if (BitmapPages.get(i).equals(col)) {
				count++;
			}
			if (count == pageNo)
				break;
		}
		BitmapPages.remove(i);
		if (!BitmapPages.contains(col)) {
			bitmappedCols.remove(col);
		}
		File toBeDeleted = new File(tableName + "B " + col + pageNo + ".class");

		if (toBeDeleted.delete()) {
			System.out.println("File" + pageNo + "Deleted");
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
			// pages.set(i, pages.get(i));
			Page currentPage = this.readPage(i + 1);
			this.writePage(currentPage, i);
		}
		File toBeDeleted = new File(tableName + " P" + pages.size() + ".class");
		if (toBeDeleted.delete()) {
			System.out.println("File" + pages.size() + "Deleted");
		}
	}

	public void shiftBitmapPagesUp(int startPage, String col) {
		BitmapPages.remove(startPage);
		for (int i = startPage; BitmapPages.get(i).equals(col) && i < BitmapPages.size(); i++) {
			// BitmapPages.set(i,i);
			BitMapPage currentPage = this.readBitmapPage(i + 1, col);
			this.writeBitmapPage(currentPage, i, col);
		}
		File toBeDeleted = new File(tableName + "B" + pages.size() + ".class");
		if (toBeDeleted.delete()) {
			System.out.println("File" + pages.size() + "Deleted");
		}
	}

	public void insertSortedBitmap(BitmapObject tupleToInsert, String colName) throws DBAppException {
		BitMapPage currentPage = null;
		int i = 0;
		boolean first = false;
		boolean start = false;

		int[] pageTupleNo;

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
					// throw new DBAppException("Duplicate Insertion");
				}
				if (tempVector.get(j).compareTo(tupleToInsert) > 0) {
					if (j == 0 && i > 0) {
						BitMapPage previousPage = readBitmapPage(i - 1, colName);
						previousPage.addTuple(tupleToInsert);
						previousPage.sort();
						if (tempVector.size() > maxRows) {
							BitmapObject overFlowTuple = tempVector.remove(maxRows);
							writeBitmapPage(previousPage, i - 1, colName);
							shiftingPages(overFlowTuple, i - 1, colName, temp, startInPages);

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
							shiftingPages(overFlowTuple, ++i, colName, temp, startInPages);

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
		int countRows = 0;
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

					if (j == 0 && i > 0) {
						Page previousPage = readPage(i - 1);
						previousPage.addTuple(tupleToInsert);
						previousPage.sort();

						if (tempVector.size() > maxRows) {
							Tuple overFlowTuple = tempVector.remove(maxRows);
							writePage(previousPage, i - 1);
							shiftingPages(overFlowTuple, i - 1);

							bitmapHandleInsert(tupleToInsert, countRows);
						} else {
							writePage(previousPage, i - 1);
							bitmapHandleInsert(tupleToInsert, countRows);
						}
						pages.set(i - 1, previousPage.readTuples().size());
						return;
					} else {

						currentPage.addTuple(tupleToInsert);
						currentPage.sort();
						if (tempVector.size() > maxRows) {
							Tuple overFlowTuple = tempVector.remove(maxRows);
							writePage(currentPage, i);
							shiftingPages(overFlowTuple, ++i);
							countRows--;
							bitmapHandleInsert(tupleToInsert, countRows);

						} else {
							writePage(currentPage, i);
							bitmapHandleInsert(tupleToInsert, countRows);

						}
					}
					return;
				}
				countRows++;
			}
			pages.set(i, tempVector.size());
		}

		currentPage = readPage(pages.size() - 1);
		Vector<Tuple> tempVector = currentPage.readTuples();
		for (int j = 0; j < tempVector.size(); j++) {
			if (tempVector.get(j).compareTo(tupleToInsert) == 2 || tempVector.get(j).compareTo(tupleToInsert) == 0) {
				throw new DBAppException("Duplicate Insertion");
			} else {
				if (tempVector.get(j).compareTo(tupleToInsert) < 0) {
					countRows++;
				}
			}

		}
		countRows--;
		if (tempVector.size() == maxRows) {

			currentPage.addTuple(tupleToInsert);
			currentPage.sort();
			Tuple overFlow = tempVector.remove(maxRows);
			writePage(currentPage, pages.size() - 1);
			currentPage = new Page();
			pages.add(1);
			currentPage.addTuple(overFlow);
			writePage(currentPage, pages.size() - 1);
			bitmapHandleInsert(tupleToInsert, countRows);

		} else {
			if (currentPage.readTuples().size() > 0) {

				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				pages.set(pages.size() - 1, tempVector.size());
				writePage(currentPage, pages.size() - 1);
				bitmapHandleInsert(tupleToInsert, countRows);

			} else {

				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				pages.set(pages.size() - 1, tempVector.size());
				int num = 0;
				writePage(currentPage, pages.size() - 1);
				bitmapHandleInsert(tupleToInsert, countRows);

			}
		}

	}

	private void bitmapHandleInsert(Tuple tupleToInsert, int countRows) {
		boolean found = false;
		System.out.println("countrows= " + countRows);
		int pageNo = 0;
		BitmapObject newObj = null;
		int index = -1;
		for (int i = 0; i < bitmappedCols.size(); i++) {
			found = false;
			for (int k = 0; k < tupleToInsert.getColName().size(); k++) {
				if (tupleToInsert.getColName().get(k).equals(bitmappedCols.get(i))) {
					index = k;

				}
			}

			for (int j = 0; j < BitmapPages.size(); j++) {
				if (BitmapPages.get(j).equals(bitmappedCols.get(i))) {
					BitMapPage bp = readBitmapPage(pageNo, bitmappedCols.get(i));
					Vector<BitmapObject> vec = bp.readTuples();
					boolean first = true;
					for (int k = 0; k < vec.size(); k++) {
						if (tupleToInsert.getAttributes().get(index).equals(vec.get(k).getColValue())) {
							found = true;
							String b = vec.get(k).getBitmap();
							StringBuilder str = new StringBuilder(b);
							System.out.println("Before=  " + str);
							str.insert(countRows + 1, '1');
							System.out.println("After= " + str);

							vec.get(k).setBitmap(str + "");
						} else {
							String b = vec.get(k).getBitmap();
							StringBuilder str = new StringBuilder(b);
							System.out.println(str);
							str.insert(countRows + 1, '0');

							System.out.println(str);
							vec.get(k).setBitmap(str + "");

						}
					}
					this.writeBitmapPage(bp, pageNo, bitmappedCols.get(i));
					pageNo++;
				}
			}
			pageNo = 0;
			if (!found) {
				newObj = new BitmapObject(tupleToInsert.getAttributes().get(index), "");
				for (int n = 0; n < pages.size(); n++) { // loop over all pages
					Vector currentTuples = readPage(n).readTuples();

					for (int j = 0; j < currentTuples.size(); j++) { // loop over all tuples per page

						Tuple tuple = ((Tuple) currentTuples.get(j));
						int colIndex = tuple.getColName().indexOf(bitmappedCols.get(i));
						Object colValue = tuple.getAttributes().get(colIndex);
						if (tupleToInsert.getAttributes().get(index).equals(colValue)) {
							newObj.setBitmap(newObj.getBitmap() + "1");
						} else {
							newObj.setBitmap(newObj.getBitmap() + "0");
						}

					}

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

	public ArrayList getUniqueValues(String strColName) {
		ArrayList<BitmapObject> uniqueValues = new ArrayList<BitmapObject>();
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
		return uniqueValues;
	}

	public void createBitmapIndex(String strColName) throws DBAppException {
		ArrayList<BitmapObject> uniqueValues = new ArrayList<BitmapObject>();

		this.updateMeta(strColName);

		bitmappedCols.add(strColName);

		// Retrieved all unique values in column needed
		uniqueValues = getUniqueValues(strColName);
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
			pages.add(1);
			currentPage.addTuple(overFlowTuple);
			writePage(currentPage, index);
		} else {
			Page currentPage = readPage(index);
			if (currentPage.readTuples().size() < maxRows) {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				pages.set(index, currentPage.readTuples().size());
				writePage(currentPage, index);
			} else {
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				Tuple newOverFlow = (Tuple) currentPage.readTuples().remove(maxRows);
				pages.set(index, currentPage.readTuples().size());
				writePage(currentPage, index);
				shiftingPages(newOverFlow, ++index);
			}

		}
	}

	public void shiftingPages(BitmapObject overFlowTuple, int index, String colName, ArrayList<String> temp,
			int startIndex) {
		System.out.println(temp + "In Shift");
		if (index >= temp.size()) {
			int pageNo = temp.size();
			BitMapPage currentPage = new BitMapPage();
			BitmapPages.add(temp.size() + startIndex, colName);
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
				shiftingPages(newOverFlow, ++index, colName, temp, startIndex);
			}

		}

	}

	public String queryIndexed(SQLTerm sqlTerm) throws DBAppException {
		String colName = sqlTerm._strColumnName;
		String op = sqlTerm._strOperator;
		Object value = sqlTerm._objValue;
		boolean found = false;
		String tempResult = "";
		int rowCount = 0;
		for (int i = 0; i < pages.size(); i++) {
			rowCount = rowCount + pages.get(i);
		}
		for (int i = 0; i < rowCount; i++) {
			tempResult += "0";
		}
		boolean start = false;
		int first = 0;
		BitmapObject queriedValue = new BitmapObject(value, "");
		for (int i = 0; i < BitmapPages.size(); i++) {
			if (BitmapPages.get(i).equals(colName) && !found) {
				first = i;
				found = true;
			} else if (found && !((BitmapPages.get(i)).equals(colName))) {
				break;
			}
			BitMapPage currentPage = readBitmapPage(i - first, colName);
			Vector<BitmapObject> vec = currentPage.readTuples();
			for (int j = 0; j < vec.size(); j++) {
				BitmapObject currentValue = vec.get(j);
				switch (op) {
				case ">":
					if (currentValue.compareTo(queriedValue) == 1 && !start) {
						start = true;
						tempResult = currentValue.getBitmap();
					} else if (currentValue.compareTo(queriedValue) == 1) {
						tempResult = currentValue.orBitmap(tempResult);
					}
					break;
				case ">=":
					if (currentValue.compareTo(queriedValue) > 0 && !start) {
						start = true;
						tempResult = currentValue.getBitmap();
					} else if (currentValue.compareTo(queriedValue) > 0) {
						tempResult = currentValue.orBitmap(tempResult);
					}
					break;
				case "<":
					if (currentValue.compareTo(queriedValue) < 0 && !start) {
						start = true;
						tempResult = currentValue.getBitmap();
					} else if (currentValue.compareTo(queriedValue) < 0) {
						tempResult = currentValue.orBitmap(tempResult);
					}
					break;
				case "<=":
					if ((currentValue.compareTo(queriedValue) < 0 || currentValue.compareTo(queriedValue) == 2)
							&& !start) {
						start = true;
						tempResult = currentValue.getBitmap();
					} else if ((currentValue.compareTo(queriedValue) < 0
							|| currentValue.compareTo(queriedValue) == 2)) {
						tempResult = currentValue.orBitmap(tempResult);
					}
					break;
				case "=":
					if (currentValue.compareTo(queriedValue) == 2) {
						tempResult = currentValue.getBitmap();
					}
					break;
				case "!=":
					if (currentValue.compareTo(queriedValue) != 2 && !start) {
						start = true;
						tempResult = currentValue.getBitmap();
					} else if (currentValue.compareTo(queriedValue) != 2) {
						tempResult = currentValue.orBitmap(tempResult);
					}
					break;
				default:
					throw new DBAppException("Invalid op");
				}

			}
		}
		if (!columnNames.contains(colName)) {
			throw new DBAppException("Column Name not found");
		}
		return tempResult;

	}

	public static int compareObjects(Object tuple2, Object tuple1) {
		boolean flag = true;

		try {
			Double thisAttr = Double.parseDouble("" + tuple2);
			Double otherAttr = Double.parseDouble("" + tuple1);
			if (thisAttr > otherAttr) {
				return 1;
			} else if (thisAttr < otherAttr) {
				return -1;
			} else {
				return 2;
			}
		} catch (NumberFormatException e) {
			String thisAttr = ("" + tuple2);
			String otherAttr = ("" + tuple1);
			if (thisAttr.compareTo(otherAttr) > 0) {
				return 1;
			} else if ((thisAttr).compareTo(otherAttr) < 0) {
				return -1;
			} else
				return 2;
		} catch (ArrayIndexOutOfBoundsException e) {
			return 1;
		}
	}

	public String queryNormal(SQLTerm sqlTerm) throws DBAppException {
		String colName = sqlTerm._strColumnName;
		String op = sqlTerm._strOperator;
		Object queriedValue = sqlTerm._objValue;
		String result = "";
		if (!columnNames.contains(colName)) {
			throw new DBAppException("Column Name not found");
		}
		for (int i = 0; i < pages.size(); i++) { // loop over all pages
			Vector<Tuple> currentTuples = readPage(i).readTuples();
			for (int j = 0; j < currentTuples.size(); j++) { // loop over all tuples per page
				Object currentValue = currentTuples.get(j).getAttributes()
						.get(currentTuples.get(j).getColName().indexOf(colName));
				switch (op) {
				case ">":
					if (compareObjects(currentValue, queriedValue) == 1) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				case ">=":
					if (compareObjects(currentValue, queriedValue) > 0) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				case "<":
					if (compareObjects(currentValue, queriedValue) < 0) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				case "<=":
					if (compareObjects(currentValue, queriedValue) < 0
							|| compareObjects(currentValue, queriedValue) == 2) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				case "=":
					if (compareObjects(currentValue, queriedValue) == 2) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				case "!=":
					if (compareObjects(currentValue, queriedValue) != 2) {
						result += "1";
					} else {
						result += "0";
					}
					break;
				default:
					throw new DBAppException("Invalid op");
				}
			}
		}
		return result;
	}

	public Vector<Tuple> getVectorResult(String temp) {
		int index = 0;
		Vector<Tuple> result = new Vector<Tuple>();
		for (int i = 0; i < pages.size(); i++) { // loop over all pages
			Vector<Tuple> currentTuples = readPage(i).readTuples();
			for (int j = 0; j < currentTuples.size(); j++) { // loop over all tuples per page
				if (temp.charAt(index) == '1') {
					result.add(currentTuples.get(j));
				}
				index++;
			}

		}
		return result;
	}

	public boolean isIndexed(String colName) {
		// TODO Auto-generated method stub
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("./data/metaData.csv")));
			reader.readLine();
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split(",");
				if ((colName.equals(parts[1]))) {
					if (parts[parts.length - 1].equals("TRUE")) {
						return true;
					}
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
		// if (BitmapPages.contains(colName)) {
		// return true;
		// } else {
		// return false;
		// }
	}

	public static void main(String[] args) {
	}

}
