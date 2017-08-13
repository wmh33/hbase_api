package api;

import java.io.IOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseAPI extends HbaseInit{
	/***
	 * 删除hbase中的表
	 * @param tableName 表名
	 */
	public void DeleteTable(String tableName){
		TableName tName = TableName.valueOf(tableName);
		try {
			if(admin.tableExists(tName)){
				System.out.println(tableName + " 表存在");
				admin.disableTable(tName);
				admin.deleteTable(tName);
				System.out.println(tableName + " 表已删除");
			} else{
				System.out.println(tableName + " 表不存在，无需删除");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * TableName 设置表名
	 * HtableDescriptor 表的schema，可指定最大的region size（setMaxFileSize）
	 *                              指定memstore flush到HDFS上的文件大小（setMemStoreFlushSize）
	 * HColumnDescriptor 列簇的schema，指定最大的TTL,单位是ms,过期数据会被自动删除（setTimeToLive）
	 * 								指定是否放在内存中，对小表有用，可用于提高效率。默认关闭（setInMemory）
	 * 								指定是否使用BloomFilter,可提高随机查询效率。默认关闭（setBloomFilter）
	 * 								设定数据压缩类型。默认无压缩（setCompressionType）
	 * 								指定数据最大保存的版本个数（setMaxVersions）
	 * @param tableName 表名
	 * @param hColumns 列簇的数组
	 */
	public void CreateTable(String tableName, String[] hColumns){
		TableName tName = TableName.valueOf(tableName); 
		HTableDescriptor hDescriptor = new HTableDescriptor(tName); 
		HColumnDescriptor[] hColumnDescriptors = new HColumnDescriptor[hColumns.length];
		for(int i=0; i < hColumns.length; i++){
			hColumnDescriptors[i] = new HColumnDescriptor(hColumns[i]);
		}
		for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
			hDescriptor.addFamily(hColumnDescriptor);
		}
		try {
			admin.createTable(hDescriptor);
			System.out.println(tableName + " 表创建成功");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(tableName + " 表创建失败");
		}
	}
	/***
	 * 创建hbase表并以SPLITS=>[]的方式设置预分区
	 * @param tableName 表名
	 * @param hColumns 列簇名数组
	 * @param splits rowkey预分区数组
	 */
	public void CreateTable(String tableName, String[] hColumns, String[] splits){
		TableName tName = TableName.valueOf(tableName); 
		HTableDescriptor hDescriptor = new HTableDescriptor(tName); 
		HColumnDescriptor[] hColumnDescriptors = new HColumnDescriptor[hColumns.length];
		for(int i=0; i < hColumns.length; i++){
			hColumnDescriptors[i] = new HColumnDescriptor(hColumns[i]);
		}
		for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
			hDescriptor.addFamily(hColumnDescriptor);
		}
		try {
			byte[][] bs = new byte[splits.length][];
			for(int i=0; i < splits.length; i++){
				bs[i] = Bytes.toBytes(splits[i]);
			}
			admin.createTable(hDescriptor, bs);
			System.out.println(tableName + " 表创建成功");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(tableName + " 表创建失败");
		}
	}
	/***
	 * 创建hbase表，并以设置rowkey的start、end、num的方式来设置分区
	 * @param tableName 表名
	 * @param hColumns 列簇名数组
	 * @param start 第一个region的end key
	 * @param end 最后一个region的start key
	 * @param num region个数
	 */
	public void CreateTable(String tableName, String[] hColumns, String start, String end, int num){
		TableName tName = TableName.valueOf(tableName); 
		HTableDescriptor hDescriptor = new HTableDescriptor(tName); 
		HColumnDescriptor[] hColumnDescriptors = new HColumnDescriptor[hColumns.length];
		for(int i=0; i < hColumns.length; i++){
			hColumnDescriptors[i] = new HColumnDescriptor(hColumns[i]);
		}
		for (HColumnDescriptor hColumnDescriptor : hColumnDescriptors) {
			hDescriptor.addFamily(hColumnDescriptor);
		}
		try {
			admin.createTable(hDescriptor, Bytes.toBytes(start), Bytes.toBytes(end), num);
			System.out.println(tableName + " 表创建成功");
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println(tableName + " 表创建失败");
		}
	}
	/***
	 * 获取hbase中的表名，相当于shell中的list
	 * @return 返回hbase中所有的表名组成的数组，如果没有，则返回null
	 */
	public String[] GetAllTableNames(){
		TableName[] tableNames;
		String[] tablenames = null;
		try {
			tableNames = admin.listTableNames();
			if(tableNames.length != 0){
				tablenames = new String[tableNames.length];
				for (int i=0; i < tableNames.length; i++) {
					tablenames[i] = tableNames[i].toString();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tablenames;
	}
}
