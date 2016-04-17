package org.apache.hadoop.io;

public class IntWritable {

	private int value;
	
	public IntWritable(int value) {
		this.value = value;
	}
	
	public IntWritable(String value) {
		this.value =  Integer.parseInt(value);
	}
	
	public int get() {
		return this.value;
	}

	public synchronized IntWritable increment(int i) {
		this.value += i;
		return this;
	}
	
	@Override
	public String toString() {
		return String.valueOf(value);
	}
}
