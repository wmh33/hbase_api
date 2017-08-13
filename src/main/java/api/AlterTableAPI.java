package api;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class AlterTableAPI extends HbaseInit{
	/***
	 * 设置表中某个列簇的版本 
	 * @param tableName 表名
	 * @param family 列簇名
	 * @param version 版本大小
	 */
	public void SetVersion(String tableName, String family,int version){
		TableName tName = TableName.valueOf(tableName);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
		try {
			if(admin.isTableEnabled(tName)){
				admin.disableTable(tName);
			}
			admin.modifyColumn(tName, hColumnDescriptor.setMaxVersions(version));
			admin.enableTable(tName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 向hbase中的某个表添加一个列簇
	 * @param tableName 表名
	 * @param family 列簇名
	 */
	public void AddFamily(String tableName, String family){
		TableName tName = TableName.valueOf(tableName);
		try {
			if(!admin.getTableDescriptor(tName).hasFamily(Bytes.toBytes(family))){
				admin.addColumn(tName, new HColumnDescriptor(family));
			} else{
				System.out.println(tableName + "表中已存在列簇" + family);
			}		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 删除hbase中某个表的列簇
	 * @param tableName 表名
	 * @param family 列簇名
	 */
	public void RemoveFamily(String tableName, String family){
		TableName tName = TableName.valueOf(tableName);
		try {
			if(admin.getTableDescriptor(tName).hasFamily(Bytes.toBytes(family))){
				admin.deleteColumn(tName, Bytes.toBytes(family));
			} else{
				System.out.println(tableName + "表中不存在列簇" + family);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
