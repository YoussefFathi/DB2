package mashayet;

import java.io.Serializable;
import java.util.ArrayList;

public class Tuple implements Serializable {
	private ArrayList attrs = new ArrayList();
	public Tuple(ArrayList attrs) {
		this.attrs=attrs;
	}
	public ArrayList getAttributes() {
		return attrs;
	}
	public void setAttributes(ArrayList attrs) {
		this.attrs=attrs;
	}
}
