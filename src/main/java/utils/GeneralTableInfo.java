package utils;

import java.util.List;

public class GeneralTableInfo {
	private String rowkey;	
	private List<ColumnValue> columnValue;
	public GeneralTableInfo() {	}
	public GeneralTableInfo(String rowkey, List<ColumnValue> columnValue){
		this.rowkey = rowkey;
		this.columnValue = columnValue;
	}
	public String getRowkey() {
		return rowkey;
	}
	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}
	public List<ColumnValue> getColumnValue() {
		return columnValue;
	}
	public void setColumnValue(List<ColumnValue> columnValue) {
		this.columnValue = columnValue;
	}
}
