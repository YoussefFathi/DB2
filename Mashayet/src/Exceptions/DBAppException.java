package Exceptions;

public class DBAppException extends Exception {
	public DBAppException(String message) {
		System.out.println(message);
	}
}
