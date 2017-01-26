package com.i4hq.flame.mongo;

/**
 * This class is used to represent a mutable variable inside an anonymous class defined within a method.
 *  @author rmoten
 *
 * @param <T>
 */
class ValueRef<T extends Object>{
	private T value;

	
	public ValueRef() {
		super();
	}

	public ValueRef(T value) {
		super();
		this.value = value;
	}

	/**
	 * @return the value
	 */
	public T getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(T value) {
		this.value = value;
	}
	
}