/**
Contributors: Nachi
*/
package com.shieldsquare.sdk.exceptions;
public class MongoUtilException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6788562893029358585L;

	public MongoUtilException() {
	}

	public MongoUtilException(String message) {
		super(message);
	}

	public MongoUtilException(Throwable cause) {
		super(cause);
	}

	public MongoUtilException(String message, Throwable cause) {
		super(message, cause);
	}

}
