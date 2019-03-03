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

import Exceptions.DBAppException;

public class Table implements Serializable {

//	 private static final long serialVersionUID = 1L;
	transient private ArrayList<Integer> pages = new ArrayList();
	private String tableName = "";
	private final int maxRows = 2;
	private int noRows = 0;
	private String tableKey = "";
	private int attrNo = 0;
	private ArrayList columnNames = new ArrayList();

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
	}

	public void addToMeta(String key, Hashtable<String, String> table) throws IOException {
		FileWriter writer = new FileWriter(new File("metaData.csv"));
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
	public void updateTuple(Object key,Hashtable<String, Object> htblColNameValue) {
		for(int i =0;i<pages.size();i++) {
			Page tempPage = readPage(pages.get(i));
			Vector tuples = tempPage.readTuples();
			for(int j =0;j<tuples.size();j++) {
				if(((Tuple)tuples.get(j)).getAttributes().contains(key)) {
					ArrayList attrs = getArrayFromHash(htblColNameValue);
					Tuple removed=(Tuple)tuples.remove(j);
					removed.setAttributes(attrs);
					this.writePage(tempPage, i);
					insertSortedTuple(htblColNameValue);
					readPage(i);
					return;
				}
			}
		}
	}
	@SuppressWarnings("unchecked")
	public boolean findKey(String key, Page page) {
		boolean found=false;
		for(int i =0;i<page.readTuples().size();i++) {
			if(((Tuple)page.readTuples().get(i)).getAttributes().contains(key)) {
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
		 int key=0;
		for(String name : names) {
			Object value = htblColNameValue.get(name);
//			System.out.println(name +value);
			if (checkType(name, value)) {
				attrs.add( value);
				if(name.equals(tableKey)){key=attrs.size();}
			}else {
				System.out.println("Invalid Input for"+ name + " "+ value);
				return;
			}

		}
		currentPage.addTuple(new Tuple(attrs,key,null));
		writePage(currentPage, pageNo);
		readPage(pageNo);
		noRows++;

	}
//	public Class<?> getType(String name) {
//		switch (name) {
//		case "java.lang.Integer":return int.class;
//		case "java.lang.String":return String.class;
//		case "java.lang.Double":return double.class;
//		case "java.lang.Boolean":return boolean.class;
//		case "java.util.Date":return Date.class;
//		default:return null;
//		}
//	}

	public boolean checkType(String name, Object value) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File("metaData.csv")));
			reader.readLine();
			String line="";
			while((line=reader.readLine())!=null) {
				String[] parts = line.split(",");
				if((name.equals(parts[1]))&& (value.getClass().getName().equals(parts[2]))) {
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
			FileOutputStream fileOut = new FileOutputStream(tableName + " P" + indicator + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.println("Serialized data is saved in " + tableName + " P" + indicator + ".class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Page readPage(int indicator) {

		try {
			FileInputStream fileIn = new FileInputStream(tableName + " P" + indicator + ".class");
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
	public void insertSortedTuple(Hashtable<String, Object> htblColNameValue) {
		int pageNo = 0;
		ArrayList attrs = new ArrayList(attrNo);
		ArrayList colNames=new ArrayList();
		 Set<String> names = htblColNameValue.keySet();
		 int key=0;
		for(String name : names) {
			Object value = htblColNameValue.get(name);
//			System.out.println(name +value);
			if (checkType(name, value)) {
				attrs.add( value);
				colNames.add(name);
				if(name.equals(tableKey)){key=attrs.size()-1;}
			}else {
				System.out.println("Invalid Input for"+ name + " "+ value);
				return;
			}

		}
		Tuple tupleToInsert=new Tuple(attrs,key,colNames);
		Page currentPage = null;
		for(int i=0;i<pages.size()-1;i++){
			currentPage=readPage(i);
			Vector <Tuple> tempVector= currentPage.readTuples();
			for(int j=0;j<tempVector.size();j++){
				if(tempVector.get(j).compareTo(tupleToInsert)>=0){
					if(j==0 && i>0){
					Page previousPage=readPage(i-1);
					previousPage.addTuple(tupleToInsert);
					previousPage.sort();
					if(tempVector.size()>maxRows){
						Tuple overFlowTuple=tempVector.remove(maxRows);
						writePage(previousPage,i-1);
						shiftingPages(overFlowTuple,i-1);
						
					}
					else{
						writePage(previousPage,i-1);
						
						
					}return;
					}
					else{
					currentPage.addTuple(tupleToInsert);
					currentPage.sort();
					if(tempVector.size()>maxRows){
						Tuple overFlowTuple=tempVector.remove(maxRows);
						writePage(currentPage,i);
						shiftingPages(overFlowTuple,++i);
						
					}
					else{
						writePage(currentPage,i);
						
						
					}}
				return;}
			}
			
		}
		
			currentPage = readPage(pages.size()-1);
			Vector <Tuple> tempVector= currentPage.readTuples();
			if(tempVector.size()==maxRows){
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				Tuple overFlow=tempVector.remove(maxRows);
				writePage(currentPage,pages.size()-1);
				currentPage = new Page();
				pages.add(pages.size());
				currentPage.addTuple(overFlow);
				writePage(currentPage, pages.size()-1);
				
			}
			else {
				if(currentPage.readTuples().size()>0){
					currentPage.addTuple(tupleToInsert);
					currentPage.sort();
					writePage(currentPage, pages.size()-1);
						
				}
				else{
				currentPage.addTuple(tupleToInsert);
				currentPage.sort();
				int num=0;
				writePage(currentPage, pages.size()-1);
				}
			}
	}
	public ArrayList<Integer> getPages() {
		return pages;
	}

	public void shiftingPages(Tuple overFlowTuple,int index){
		if (index>=pages.size()){
			int pageNo= pages.size();
		 Page currentPage = new Page();
			pages.add(pageNo);
			currentPage.addTuple(overFlowTuple);
			writePage(currentPage,index);
		}
		else {
			Page currentPage=readPage(index);
			if(currentPage.readTuples().size()<maxRows){
				currentPage.addTuple(overFlowTuple);
				currentPage.sort();
				writePage(currentPage,index);
			}
			else
			{
			currentPage.addTuple(overFlowTuple);
			currentPage.sort();
			Tuple newOverFlow=(Tuple) currentPage.readTuples().remove(maxRows);
			writePage(currentPage,index);
			shiftingPages(newOverFlow,++index);
			}
			
		}
		
	}
	public static void main(String[] args) {
	}

}
