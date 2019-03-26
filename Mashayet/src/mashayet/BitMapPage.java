package mashayet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

public class BitMapPage implements Serializable  {

	
	private Vector<BitmapObject> bitmapObjects = new Vector();

	public void addTuple(BitmapObject bmo) {
		bitmapObjects.add(bmo);
	}
	public Vector readTuples() {
		return bitmapObjects;
	}
	
	public void sort(){
		Collections.sort(bitmapObjects);
	}
	public static void main(String[] args) {
		System.out.println("00000000".contains("1"));
	}

}
