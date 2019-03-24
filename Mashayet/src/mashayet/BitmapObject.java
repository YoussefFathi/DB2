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
		// TODO Auto-generated method stub

	}
	@Override
	public int compareTo(Object BitObject) {
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

}
