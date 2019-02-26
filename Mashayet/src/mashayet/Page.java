package mashayet;

import java.io.Serializable;
import java.io.*;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = 1214738099184642172L;
	private Vector<String> tuples = new Vector();

	public void addTuple(String tuple) {
		tuples.add(tuple);
	}
	public Vector readTuples() {
		return tuples;
	}
	public long getID() {
		return serialVersionUID;
	}
	public static void main(String[] args) {
		

	}

}
