package com.jd.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey> {
	private String first;
	private String second;

	public CompositeKey() {
	}

	public CompositeKey(String first, String second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public String toString() {
		return (new StringBuilder()).append(first).append('\t').append(second)
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = WritableUtils.readString(in);
		second = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, first);
		WritableUtils.writeString(out, second);
	}

	@Override
	public int compareTo(CompositeKey o) {
		int result = first.compareTo(o.first);
		if (0 == result) {
			result = second.compareTo(o.second);
		}
		return result;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}
}
