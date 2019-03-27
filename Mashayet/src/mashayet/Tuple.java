package mashayet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class Tuple implements Serializable, Comparable {
	private int keyIndex;
	private ArrayList attrs = new ArrayList();
	private ArrayList colName = new ArrayList();

	public Tuple(ArrayList attrs, int keyIndex, ArrayList colName) {
		this.attrs = attrs;
		this.keyIndex = keyIndex;
		this.colName = colName;
		attrs.add(new Date());
		colName.add("TouchDate");
	}
	public void updateDate() {
		attrs.set(attrs.size()-1, new Date());
	}
	public ArrayList getAttributes() {
		return attrs;
	}

	public void setAttributes(ArrayList attrs) {
		this.attrs = attrs;
	}

	@Override
	public int compareTo(Object tuple1) {
		boolean flag = true;
		Tuple min = (((Tuple) tuple1).attrs.size() >= this.attrs.size()) ? this : (Tuple) tuple1;
		Tuple max = (((Tuple) tuple1).attrs.size() < this.attrs.size()) ? this : (Tuple) tuple1;
		for (int i = 0; i < min.attrs.size()-1; i++) {
			for (int j = 0; j < max.attrs.size()-1; j++) {
				if (min.colName.get(i).equals(max.colName.get(j))) {
					if (!min.attrs.get(i).equals(max.attrs.get(j))) {
						flag = false;
					}
				}
			}
		}
		if (flag) {
			return 0;
		}
		try {
			Double thisAttr = Double.parseDouble("" + this.attrs.get(keyIndex));
			Double otherAttr = Double.parseDouble(("" + ((Tuple) tuple1).attrs.get(((Tuple) tuple1).getKeyIndex())));
			if (thisAttr > otherAttr) {
				return 1;
			} else if (thisAttr < otherAttr) {
				return -1;
			} else {
				return 2;
			}
		} catch (NumberFormatException e) {
			String thisAttr = ("" + this.attrs.get(keyIndex));
			String otherAttr = ("" + ((Tuple) tuple1).attrs.get(((Tuple) tuple1).getKeyIndex()));
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

	public int getKeyIndex() {
		return keyIndex;
	}

	public String toString() {
		String e = "";
		for (int i = 0; i < attrs.size(); i++) {
			e = e + " " + attrs.get(i);
		}
		return e;

	}

	public static void main(String[] args) {
		ArrayList a1 = new ArrayList();
		a1.add(2);
		a1.add("Ahmed Noor");
		a1.add("2.5");
		ArrayList b1 = new ArrayList();
		b1.add(2);
		b1.add("Ahmed oor");
		b1.add("2.5");
		ArrayList a2 = new ArrayList();
		a2.add("id");
		a2.add("name");
		a2.add("gpa");
		ArrayList b2 = new ArrayList();
		b2.add("id");
		b2.add("name");
		b2.add("gpa");
		Tuple t1 = new Tuple(a1, 0, a2);
		Tuple t2 = new Tuple(b1, 0, b2);
		System.out.println(t1.compareTo(t2));

	}

	public ArrayList getColName() {
		return colName;
	}

	public void setColName(ArrayList colName) {
		this.colName = colName;
	}

	public void setKeyIndex(int keyIndex) {
		this.keyIndex = keyIndex;
	}
}
