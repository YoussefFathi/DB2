package mashayet;

import java.io.Serializable;
import java.util.ArrayList;

public class Tuple implements Serializable, Comparable {
	private int keyIndex;
	private ArrayList attrs = new ArrayList();
	public Tuple(ArrayList attrs,int keyIndex) {
		this.attrs=attrs;
		this.keyIndex=keyIndex;
	}
	public ArrayList getAttributes() {
		return attrs;
	}
	public void setAttributes(ArrayList attrs) {
		this.attrs=attrs;
	}
	@Override
	public int compareTo(Object tuple1) {
		try{
			int thisAttr=Integer.parseInt(""+this.attrs.get(keyIndex));
			int otherAttr=Integer.parseInt((""+((Tuple)tuple1).attrs.get(keyIndex)));
			if(thisAttr>otherAttr){return 1;}
			else if(thisAttr<otherAttr){return -1;}
			else{
				for(int i=0;i<attrs.size();i++){
					if(!((Tuple)tuple1).attrs.get(i).equals(this.attrs.get(i))){
						return 1;
					}
				}return 0;}
		}catch (NumberFormatException e){
			String thisAttr=(""+this.attrs.get(keyIndex));
			String otherAttr=(""+((Tuple)tuple1).attrs.get(keyIndex));
			if(thisAttr.compareTo(otherAttr)>0){return 1;}
			else if((thisAttr).compareTo(otherAttr)<0){return -1;}
			else{
				for(int i=0;i<attrs.size();i++){
					if(!((Tuple)tuple1).attrs.get(i).equals(this.attrs.get(i))){
						return 1;
					}
				}return 0;}
		}
		
	}
	public String toString(){
		String e="";
		for(int i=0;i<attrs.size();i++){
			e=e+" "+attrs.get(i);
		}
		return e;
		
	}
}
