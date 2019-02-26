package mashayet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Table implements Serializable {

	// private static final long serialVersionUID = 1L;
	transient private ArrayList pages = new ArrayList();
	private String tableName="";
	private  final int maxRows = 5;
	public Table(String tableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) {
		this.tableName=tableName;
		try {
			this.addToMeta(strClusteringKeyColumn,htblColNameType);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Page first = new Page();
		pages.add((long) first.getID());
		this.writePage(first);
	}
	public void addToMeta(String key,Hashtable<String, String> table) throws IOException {
		FileWriter writer= new FileWriter(new File("metaData.csv"));
		try {
			writer.append("Table Name, Column Name, Column Type, Key,Indexed ");
			writer.append('\n');
			table.forEach((name,type)->{
				try {
					writer.append(this.tableName);
					writer.append(',');
					writer.append(name);
					writer.append(',');
					writer.append(type);
					writer.append(',');
					if(key.equals(name)) {
						writer.append("True");
					}else {
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
		}
		finally {
			writer.flush();
			writer.close();
		}
	}
	public void writePage(Page page) {

		try {
			FileOutputStream fileOut = new FileOutputStream("Page.class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
			System.out.printf("Serialized data is saved in Page.class");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Page readPage() {

		try {
			FileInputStream fileIn = new FileInputStream("Page.class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			Page e = (Page) in.readObject();
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

	public static void main(String[] args) {
		
		

	}

}
