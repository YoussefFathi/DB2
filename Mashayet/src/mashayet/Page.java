package mashayet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Vector;

public class Page implements Serializable {

	private static final long serialVersionUID = 1214738099184642172L;
	private Vector<Tuple> tuples = new Vector();

	public void addTuple(Tuple tuple) {
		tuples.add(tuple);
	}
	public Vector readTuples() {
		return tuples;
	}
	public long getID() {
		return serialVersionUID;
	}
	public void sort(){
		Collections.sort(tuples);
	}
	public static void main(String[] args) {
		
	}

}
