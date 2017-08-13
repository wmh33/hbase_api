package utils;

public class ColumnValue {
	private String family;
	private String column;
	private String value;
	private long timestamp;
	public ColumnValue() {
	}
	public ColumnValue(String family, String column, String value) {
		this.family = family;
		this.column = column;
		this.value = value;
		this.timestamp = System.currentTimeMillis();
	}
	public ColumnValue(String family, String column, String value, long timestamp) {
		this.family = family;
		this.column = column;
		this.value = value;
		this.timestamp = timestamp;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getColumn() {
		return column;
	}
	public void setColumn(String column) {
		this.column = column;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
