package org.nutz.ssdb4j.spi;

public class KeyValue {

	private String key;

	private String value;

	private long score;

	public KeyValue(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public KeyValue(String key, long score) {
		this.key = key;
		this.score = score;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getScore() {
		return score;
	}

	public void setScore(long score) {
		this.score = score;
	}
}
