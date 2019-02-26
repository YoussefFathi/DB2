package mashayet;

import java.io.Serializable;
import java.util.Vector;
public class Page implements Serializable {
	private Vector<String> tuples = new Vector();
	public void write() {
		 try {
	         FileOutputStream fileOut =
	         new FileOutputStream("/tmp/employee.ser");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(e);
	         out.close();
	         fileOut.close();
	         System.out.printf("Serialized data is saved in /tmp/employee.ser");
	      } catch (IOException i) {
	         i.printStackTrace();
	      
	public static void main(String[] args) {
		
 
	}

}
