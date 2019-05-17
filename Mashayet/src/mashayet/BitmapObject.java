package mashayet;

import java.io.Serializable;

public class BitmapObject implements Serializable, Comparable {
	private Object colValue;
	private String bitmap;
	
	public BitmapObject(Object colValue,String bitmap) {
	
		this.colValue=colValue;
		compress(bitmap);
	}
	public Object getColValue() {
		return colValue;
	}
	public void setColValue(Object colValue) {
		this.colValue = colValue;
	}
	public String getBitmap() {
		return decompress();
	}
	public void setBitmap(String bitmap) {
		this.compress(bitmap);
	}
	public static void main(String[] args) {
		BitmapObject bo=new BitmapObject("","1000000000000001");
		BitmapObject bo1=new BitmapObject("","0100000000000000");
		System.out.println(bo1.compress("1000000000000001"));
		System.out.println(bo.compress("0110000000100000"));

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
		String bm1=this.decompress();
////		String bm2=bo.getBitmap();
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
	System.out.println(bm1);
	System.out.println(bm2);
////	String bm2=bo.getBitmap();
	for(int i=0;i<bm1.length();i++){
		if(bm1.charAt(i)=='1' || bm2.charAt(i)=='1'){
			result=result+"1";
		}else
			result=result+"0";
	}
	return result;
}
//public  String decompress() {
//	String[] parts = this.bitmap.split(":");
//	String tempResult = "";
//
//	for (int i = 0; i < parts.length; i++) {
//		int tempInt = Integer.parseInt(parts[i]);
//		if (tempInt == 1|| tempInt==0) {
//			tempResult = tempResult + tempInt;
//		} else {
//			for (int k = 0; k < tempInt; k++) {
//				tempResult = tempResult + 0;
//			}
//		}
//	}
//	return tempResult;
//}
//public  String compress(String bitmap) {
//	String finalData = "";
//	char[] num = bitmap.toCharArray();
//	int zeroCount = 0;
//	boolean zeroCheck = false;
//
//	for (int i = 0; i < num.length; i++) {
//		if (num[i] == '1' ) {
//			if (zeroCheck == true) {
//				if (zeroCount == 1){
//					finalData = finalData + '0' + ":";
//					System.out.println(finalData);}
//				else{
//					finalData = finalData + zeroCount + ":";
//					System.out.println(finalData);
//					}
//				zeroCount = 0;
//				zeroCheck = false;
//			}
//			finalData = finalData + num[i] + ":";
//			System.out.println(finalData);
//		} else {
//			zeroCount++;
//			zeroCheck = true;
//		}
//
//	}
//////	finalData = finalData + zeroCount + ":";
//	this.bitmap=finalData;
//	return finalData;
//
//}
public String compress(String str){
	String acc="";
	int count=0;
	for(int i=0;i<str.length();i++){
		if(str.charAt(i)=='0'){
			count++;
		}
		else{
			if(count>0){
				acc=acc+count;
			}
			acc=acc+".";
			count=0;
		}
	}
	if(count>0){
	bitmap=acc+count;
	return acc+count;
	}
	else{
	bitmap=acc;
	return acc;}
}
public String decompress(){
	String acc="";
	String count="";
	String str=this.bitmap;
	boolean found=false;
	for(int i=0;i<str.length();i++){
		if(str.charAt(i)=='.'){
			acc=acc+"1";
		}
		while(i<str.length()&& !(str.charAt(i)=='.')){
			count=count+str.charAt(i);
			i++;
			found=true;
		}
		if(found){
		for(int c=0;c<Integer.parseInt(count);c++){
			acc=acc+"0";
			
		}
		i--;
		}
		count="";
		found=false;
	}
	return acc;
}
public String toString(){
	return colValue+": "+bitmap;
	
}
}
