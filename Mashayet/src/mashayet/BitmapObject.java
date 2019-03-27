package mashayet;

import java.io.Serializable;

public class BitmapObject implements Serializable, Comparable {
	private Object colValue;
	private String bitmap;
	
	public BitmapObject(Object colValue,String bitmap) {
	
		this.colValue=colValue;
		this.bitmap=bitmap;
	}
	public Object getColValue() {
		return colValue;
	}
	public void setColValue(Object colValue) {
		this.colValue = colValue;
	}
	public String getBitmap() {
		return bitmap;
	}
	public void setBitmap(String bitmap) {
		this.bitmap = bitmap;
	}
	public static void main(String[] args) {
//		 String b = new String("Tutorial");
//		 StringBuilder str = new StringBuilder(b);
//		 str.insert(7, 's');
//		   System.out.print("After insertion = ");
//		   System.out.println(str.toString());// this will print Tutorials
	}
	@Override
	public int compareTo(Object BitObject) {
		if(this.getColValue().equals(((BitmapObject) BitObject).getColValue())){
			return 0;
		}
		try{
			Double thisAttr=Double.parseDouble(""+colValue);
			Double otherAttr=Double.parseDouble((""+((BitmapObject)BitObject).getColValue()));
			if(thisAttr>otherAttr){return 1;}
			else if(thisAttr<otherAttr){return -1;}
			else {return 2;}
		}catch (NumberFormatException e){
			String thisAttr=(""+colValue);
			String otherAttr=((""+((BitmapObject)BitObject).getColValue()));
			if(thisAttr.compareTo(otherAttr)>0){return 1;}
			else if((thisAttr).compareTo(otherAttr)<0){return -1;}
			else return 2;
		}
		catch(ArrayIndexOutOfBoundsException e){
			return 1;
		}
	}
	public String andBitmap(String bm2){
		String result="";
		String bm1=this.getBitmap();
//		String bm2=bo.getBitmap();
		for(int i=0;i<bm1.length();i++){
			if(bm1.charAt(i)=='0' || bm2.charAt(i)=='0'){
				result=result+"0";
			}else
				result=result+"1";
		}
		return result;
	}

public String orBitmap(String bm2){
	String result="";
	String bm1=this.getBitmap();
//	String bm2=bo.getBitmap();
	for(int i=0;i<bm1.length();i++){
		if(bm1.charAt(i)=='1' || bm2.charAt(i)=='1'){
			result=result+"1";
		}else
			result=result+"0";
	}
	return result;
}
public String toString(){
	return colValue+": "+bitmap;
	
}
}
