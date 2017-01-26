package com.i4hq.flame.mongo;

public interface BulkOperation {

	/**
	 * 
	 */
	void flush();

	/**
	 * 
	 */
	void close();

	/**
	 * @param i
	 */
	void setBufferWriteThreshold(int i);

}