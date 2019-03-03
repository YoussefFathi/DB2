package mashayet;

import java.io.Serializable;
import java.util.ArrayList;
public class Tuple implements Serializable, Comparable {
	private int keyIndex;
	private ArrayList attrs = new ArrayList();
	private ArrayList colName=new ArrayList();
	public Tuple(ArrayList attrs,int keyIndex,ArrayList colName) {
		this.attrs=attrs;
		this.keyIndex=keyIndex;
		this.colName=colName;
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
			Double thisAttr=Double.parseDouble(""+this.attrs.get(keyIndex));
			Double otherAttr=Double.parseDouble((""+((Tuple)tuple1).attrs.get(((Tuple) tuple1).getKeyIndex())));
			if(thisAttr>otherAttr){return 1;}
			else if(thisAttr<otherAttr){return -1;}
			else{
				Tuple min=(((Tuple)tuple1).attrs.size()>this.attrs.size())?this:(Tuple)tuple1;
				Tuple max=(((Tuple)tuple1).attrs.size()<this.attrs.size())?this:(Tuple)tuple1;
				for(int i=0;i<min.attrs.size();i++){
				for(int j=0;j<max.attrs.size();j++){
				if(min.colName.get(i).equals(max.colName.get(j))){
					if(!min.attrs.get(i).equals(max.attrs.get(j))){
						return 1;
					}
				}	
				}
				}
				return 0;
				}
		}catch (NumberFormatException e){
			String thisAttr=(""+this.attrs.get(keyIndex));
			String otherAttr=(""+((Tuple)tuple1).attrs.get(((Tuple)tuple1).getKeyIndex()));
			if(thisAttr.compareTo(otherAttr)>0){return 1;}
			else if((thisAttr).compareTo(otherAttr)<0){return -1;}
			else{
				Tuple min=(((Tuple)tuple1).attrs.size()>this.attrs.size())?this:(Tuple)tuple1;
				Tuple max=(((Tuple)tuple1).attrs.size()<this.attrs.size())?this:(Tuple)tuple1;
				for(int i=0;i<min.attrs.size();i++){
				for(int j=0;j<max.attrs.size();j++){
				if(min.colName.get(i).equals(max.colName.get(j))){
					if(!min.attrs.get(i).equals(max.attrs.get(j))){
						return 1;
					}
				}	
				}
				}
				return 0;
				}
		}
		
	}
	public int getKeyIndex() {
		return keyIndex;
	}
	public String toString(){
		String e="";
		for(int i=0;i<attrs.size();i++){
			e=e+" "+attrs.get(i);
		}
		return e;
		
	}
	public static void main (String[] args){
		ArrayList a1=new ArrayList();
		a1.add("hi");
		a1.add(1);
		ArrayList b1=new ArrayList();
		b1.add("hi");
		b1.add("byae");
		b1.add(0);
		ArrayList a2=new ArrayList();
		a2.add("name");
		a2.add("id");
		ArrayList b2=new ArrayList();
		b2.add("name");
		b2.add("name2");
		b2.add("id");
		Tuple t1=new Tuple(a1,1,a2);
		Tuple t2=new Tuple(b1,2,b2);
		System.out.println(t1.compareTo(t2));
		
	}
}
