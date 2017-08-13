package api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import utils.ColumnValue;
import utils.GeneralTableInfo;

public class HbaseTableAPI extends HbaseInit{
	/***
	 * 查询hbase中某个表最新版本数据行数
	 * @param tableName 表名
	 * @return 行数
	 */
	public long count(String tableName){
		long rowCount = 0;
		Table table = null;
		TableName tName = TableName.valueOf(tableName);
		try {
			if(!admin.tableExists(tName)){
				System.err.println("hbase中不存在" + tableName);
				return -1;
			}
			table = conn.getTable(tName);
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			ResultScanner rScanner = table.getScanner(scan);
			for (Result result : rScanner) {
				rowCount += result.size();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("行数： " + rowCount);
		return rowCount;
	}
	/***
	 * scan扫描全表
	 * @param tableName 表名
	 * @param version 版本数
	 * @return GeneralTableInfo实例列表
	 */
	public List<GeneralTableInfo> scan(String tableName, int version){
		List<GeneralTableInfo> scanResult = new ArrayList<GeneralTableInfo>();
		Table table = null;
		TableName tName = TableName.valueOf(tableName);
		try {
			if(!admin.tableExists(tName)){
				System.err.println("hbase中不存在" + tableName);
				return null;
			}
			table = conn.getTable(tName);
			Scan scan = new Scan();
			if(version >= 1){
				scan.setMaxVersions(version);
			}
			ResultScanner rScanner = table.getScanner(scan);
			for (Result result : rScanner) {
				List<ColumnValue> res = new ArrayList<ColumnValue>();
				List<Cell> cells = result.listCells();
				for (Cell cell : cells) {
					ColumnValue cValue = new ColumnValue(new String(CellUtil.cloneFamily(cell)), 
							new String(CellUtil.cloneQualifier(cell)), 
							new String(CellUtil.cloneValue(cell)));
					res.add(cValue);
				}
				scanResult.add(new GeneralTableInfo(new String(result.getRow()), res));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return scanResult;
	}
	/***
	 * 删除hbase某个表中指定rowkey的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 */
	public void deleteData(String tableName, String rowkey){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 删除hbase某个表中指定rowkey的某一列簇下的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param family 列簇名
	 */
	public void deleteData(String tableName, String rowkey, String family){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			if(table.getTableDescriptor().hasFamily(Bytes.toBytes(family))){
				delete.addFamily(Bytes.toBytes(family));
				table.delete(delete);
			} else{
				System.err.println(tableName + "表中不存在列簇" + family);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 删除hbase某个表中指定rowkey的指定列簇下的指定列名的最新版本数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param family 列簇名
	 * @param column 列名
	 */
	public void deleteData(String tableName, String rowkey, String family, String column){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			if(table.getTableDescriptor().hasFamily(Bytes.toBytes(family)) && !"".equals(column)){
				delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
				table.delete(delete);
			} else{
				System.err.println(tableName + "表中不存在列簇" + family);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 删除hbase某个表中指定rowkey的指定列簇下的指定列名下的指定版本的数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param family 列簇名
	 * @param column 列名
	 * @param timestamp 时间戳
	 */
	public void deleteData(String tableName, String rowkey, String family, String column, long timestamp){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Delete delete = new Delete(Bytes.toBytes(rowkey));
			if(table.getTableDescriptor().hasFamily(Bytes.toBytes(family)) && !"".equals(column)){
				delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), timestamp);
				table.delete(delete);
			} else{
				System.err.println(tableName + "表中不存在列簇" + family);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 批量删除hbase某个表中指定的rowkey的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 */
	public void deleteDatas(String tableName, String[] rowkeys){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			List<Delete> deletes = new ArrayList<Delete>();
			for(int i=0; i < rowkeys.length; i++){
				deletes.add(new Delete(Bytes.toBytes(rowkeys[i])));
			}		
			table.delete(deletes);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 返回hbase表中指定rowkey下最新版本的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @return Map<String, String>：key为"列簇名:列名"，value为值
	 */
	public Map<String, String> getData(String tableName, String rowkey){
		Map<String, String> fMap = new HashMap<String, String>();
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			Result result = table.get(get);
			fMap = getFromResult(result, null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fMap;
	}
	/***
	 * 从hbase表中获取指定rowkey的指定版本数量的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param version 版本数
	 * @return Map<String, List<String>>中key为"列簇名:列名"，value为包含多个版本的list，其中每条的数据格式为"value:timestamp"
	 */
	public Map<String, List<String>> getData(String tableName, String rowkey, int version){
		Map<String, List<String>> fMap = new HashMap<String, List<String>>();
		if(version <= 1){
			version = 1;
		}
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			get.setMaxVersions(version);
			Result result = table.get(get);
			fMap = getFromResultByVersion(result, null, version);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return fMap;
	}
	/***
	 * 返回hbase表中指定rowkey下、指定列簇下的最新版本的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param family 列簇名
	 * @return Map<String, String>中key为列名，value为值
	 */
	public Map<String, String> getData(String tableName, String rowkey, String family){
		Map<String, String> fMap = new HashMap<String, String>();
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			Result result = table.get(get);
			fMap = getFromResult(result, family);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fMap;
	}
	/***
	 * 从hbase表中获取指定rowkey、指定列簇下的指定版本数量的所有数据
	 * @param tableName 表名
	 * @param rowkey 行键
	 * @param family 列簇名
	 * @param version 版本数
	 * @return Map<String, List<String>>中key为"列簇名:列名"，value为包含多个版本的list，其中每条的数据格式为"value:timestamp"
	 */
	public Map<String, List<String>> getData(String tableName, String rowkey, String family, int version){
		Map<String, List<String>> fMap = new HashMap<String, List<String>>();
		if(version <= 1){
			version = 1;
		}
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowkey));
			get.setMaxVersions(version);
			Result result = table.get(get);
			fMap = getFromResultByVersion(result, family, version);			
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return fMap;
	}
	/***
	 * 批量获取hbase表中指定rowkey的当前版本的所有数据
	 * @param tableName 表名
	 * @param rowkeys 行键数组
	 * @return Map<String, Map<String, String>>中key为rowkey，value为rowkey的当前版本所有数据(Map<String, String>,
	 * 其中key为"列簇名:列名"，value为值)
	 */
	public Map<String, Map<String, String>> getDatas(String tableName, String[] rowkeys){
		Map<String, Map<String, String>> finResult = new HashMap<String, Map<String,String>>();
		List<Get> gets = new ArrayList<Get>();
		for(int i=0; i < rowkeys.length; i++){
			gets.add(new Get(Bytes.toBytes(rowkeys[i])));
		}
		Table table;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Result[] result = table.get(gets);
			for(int i=0; i < result.length; i++){
				Map<String, String> ans = getFromResult(result[i], null);
				finResult.put(rowkeys[i], ans);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return finResult;
	}
	/***
	 * 批量获取hbase表中指定rowkey的指定版本数的所有数据
	 * @param tableName 表名
	 * @param rowkeys 行键数组
	 * @param version 版本数
	 * @return Map<String, Map<String, List<String>>>中key为rowkey，value为rowkey的所有数据(Map<String, List<String>>,
	 * 其中key为"列簇名:列名"，value为"值:timestamp")
	 */
	public Map<String, Map<String, List<String>>> getDatas(String tableName, String[] rowkeys, int version){
		Map<String, Map<String, List<String>>> finResult = new HashMap<String, Map<String,List<String>>>();
		if(version <= 1){
			version = 1;
		}
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			List<Get> gets = new ArrayList<Get>();
			for(int i = 0; i < rowkeys.length; i++){
				gets.add(new Get(Bytes.toBytes(rowkeys[i])).setMaxVersions(version));
			}
			Result[] results = table.get(gets);
			for (int i = 0; i < results.length; i++) {
				Map<String, List<String>> ans = getFromResultByVersion(results[i], null, version);
				finResult.put(rowkeys[i], ans);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return finResult;
	}
	/***
	 * 批量获取hbase表中指定rowkey、指定列簇的当前版本的所有数据
	 * @param tableName 表名
	 * @param rowkeys 行键数组
	 * @param family 列簇名
	 * @return Map<String, Map<String, String>>中key为rowkey，value为rowkey的当前版本所有数据(Map<String, String>,
	 * 其中key为"列簇名:列名"，value为值)
	 */
	public Map<String, Map<String, String>> getDatas(String tableName, String[] rowkeys, String family){
		Map<String, Map<String, String>> finResult = new HashMap<String, Map<String,String>>();
		List<Get> gets = new ArrayList<Get>();
		for(int i=0; i < rowkeys.length; i++){
			gets.add(new Get(Bytes.toBytes(rowkeys[i])));
		}
		Table table;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Result[] result = table.get(gets);
			for(int i=0; i < result.length; i++){
				Map<String, String> ans = getFromResult(result[i], family);
				finResult.put(rowkeys[i], ans);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return finResult;
	}
	/***
	 * 批量获取hbase表中指定rowkey、指定列簇的指定版本数的所有数据
	 * @param tableName 表名
	 * @param rowkeys 行键数组
	 * @param family 列簇名
	 * @param version 版本数
	 * @return Map<String, Map<String, List<String>>>中key为rowkey，value为rowkey的所有数据(Map<String, List<String>>,
	 * 其中key为"列簇名:列名"，value为"值:timestamp")
	 */
	public Map<String, Map<String, List<String>>> getDatas(String tableName, String[] rowkeys, String family,int version){
		Map<String, Map<String, List<String>>> finResult = new HashMap<String, Map<String,List<String>>>();
		if(version <= 1){
			version = 1;
		}
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			List<Get> gets = new ArrayList<Get>();
			for(int i = 0; i < rowkeys.length; i++){
				gets.add(new Get(Bytes.toBytes(rowkeys[i])).setMaxVersions(version));
			}
			Result[] results = table.get(gets);
			for (int i = 0; i < results.length; i++) {
				Map<String, List<String>> ans = getFromResultByVersion(results[i], family, version);
				finResult.put(rowkeys[i], ans);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return finResult;
	}
	/***
	 * 从result中返回指定版本数量的所有数据
	 * @param result Result实例
	 * @param family 列簇名，当列簇名为""或者null时，返回所有列簇的数据；否则返回当前列簇下的所有数据
	 * @param version 版本数
	 * @return Map<String, List<String>>中key为"列簇名:列名"，value为包含多个版本的list，其中每条的数据格式为"value:timestamp"
	 */
	private Map<String, List<String>> getFromResultByVersion(Result result, String family, int version) {
		Map<String, List<String>> fMap = new HashMap<String, List<String>>();
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersionsResult = result.getMap();
		if(family == null || family.equals("")){
			for(byte[] family_bs : allVersionsResult.keySet()){
				NavigableMap<byte[], NavigableMap<Long, byte[]>> nMap = allVersionsResult.get(family_bs);
				for(byte[] column_bs : nMap.keySet()){
					int num = 0;
					NavigableMap<Long, byte[]> ans = nMap.get(column_bs);
					List<String> value_time = new ArrayList<String>();
					for(long timestamp : ans.keySet()){
						if(num < version){
							value_time.add(new String(ans.get(timestamp)) + ":" + timestamp);
							num++;
						} else{
							break;
						}						
					}
					fMap.put(new String(family_bs) + ":" + new String(column_bs), value_time);
				}
			}
		} else{
			for(byte[] family_bs : allVersionsResult.keySet()){
				if(family.equals(new String(family_bs))){
					NavigableMap<byte[], NavigableMap<Long, byte[]>> nMap = allVersionsResult.get(family_bs);
					for(byte[] column_bs : nMap.keySet()){
						int num = 0;
						NavigableMap<Long, byte[]> ans = nMap.get(column_bs);
						List<String> value_time = new ArrayList<String>();
						for(long timestamp : ans.keySet()){
							if(num < version){
								value_time.add(new String(ans.get(timestamp)) + ":" + timestamp);
								num++;
							} else{
								break;
							}						
						}
						fMap.put(new String(family_bs) + ":" + new String(column_bs), value_time);
					}
				}				
			}
		}
		return fMap;
	}
	/***
	 * 从Result实例中获取当前版本的所有数据
	 * @param result Result实例
	 * @param family 列簇名，当列簇名为""或者null时，返回所有列簇的数据；否则返回当前列簇下的所有数据
	 * @return Map<String, String> key为"列簇名:列名"，value为值
	 */
	private Map<String, String> getFromResult(Result result, String family) {
		Map<String, String> fMap = new HashMap<String, String>();
		if(family == null || family.equals("")){
			for(Cell cell : result.listCells()){
				fMap.put(new String(CellUtil.cloneFamily(cell)) + ":" + new String(CellUtil.cloneQualifier(cell)), 
						new String(CellUtil.cloneValue(cell)));
			}
		} else{
			for(Cell cell : result.listCells()){
				if(family.equals(new String(CellUtil.cloneFamily(cell)))){
					fMap.put(family + ":" + new String(CellUtil.cloneQualifier(cell)), 
							new String(CellUtil.cloneValue(cell)));
				}
			}
		}
		
		return fMap;
	}
	/***
	 * 向hbase表中插入行键为rowkey的相关数据
	 * @param tableName 表名
	 * @param generalTableInfo 其中包含rowkey、ColumnValue列表(列表中的每一个ColumnValue实例中都包含family、column、value、timestamp
	 * 、且如果这个实例没有指定timestamp时，即为当前时间的时间戳)
	 */
	public void putData(String tableName, GeneralTableInfo generalTableInfo){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Put put = getPut(generalTableInfo);
			table.put(put);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	/***
	 * 向hbase表中批量插入数据
	 * @param tableName 表名
	 * @param generalTableInfos GeneralTableInfo实例列表
	 */
	public void putDatas(String tableName, List<GeneralTableInfo> generalTableInfos){
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			List<Put> puts = new ArrayList<Put>();
			for (GeneralTableInfo generalTableInfo : generalTableInfos) {
				puts.add(getPut(generalTableInfo));
			}
			table.put(puts);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/***
	 * 将一个GeneralTableInfo实例中的数据放入Put实例中
	 * @param generalTableInfo
	 * @return put
	 */
	private Put getPut(GeneralTableInfo generalTableInfo) {
		Put put = new Put(Bytes.toBytes(generalTableInfo.getRowkey()));
		List<ColumnValue> columnValues = generalTableInfo.getColumnValue();
		for(int i=0; i < columnValues.size(); i++){
			ColumnValue cv = columnValues.get(i);
			put.addColumn(Bytes.toBytes(cv.getFamily()), 
					Bytes.toBytes(cv.getColumn()), 
					cv.getTimestamp(), 
					Bytes.toBytes(cv.getValue()));
		}
		return put;
	}
}
