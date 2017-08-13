package testDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.sun.tools.internal.ws.wsdl.document.Import;

import api.AlterTableAPI;
import api.HbaseAPI;
import api.HbaseTableAPI;
import utils.ColumnValue;
import utils.GeneralTableInfo;

public class testHbaseAPI {
	public static void main(String[] args) {
//		HbaseAPI hbaseAPI = new HbaseAPI();
//		hbaseAPI.DeleteTable("nfmedia");
//		hbaseAPI.DeleteTable("nfmedia_test");
//		String[] hcolumns = {
//			"nfmedia"	
//		};
//		hbaseAPI.CreateTable("nfmedia_test2", hcolumns, "0", "30", 3);
//		String[] names = hbaseAPI.GetAllTableNames();
//		for (String string : names) {
//			System.out.println(string);
//		}
//		AlterTableAPI alterTableAPI = new AlterTableAPI();
//		alterTableAPI.AddFamily("nfmedia_test", "test");
//		alterTableAPI.SetVersion("nfmedia_test2", "nfmedia", 10);
//		alterTableAPI.RemoveFamily("nfmedia_test2", "nfmedia_1");
		HbaseTableAPI hApi = new HbaseTableAPI();

		List<GeneralTableInfo> ans = hApi.scan("nfmedia_test", 3);
		for (GeneralTableInfo generalTableInfo : ans) {
			List<ColumnValue> ll = generalTableInfo.getColumnValue();
			for (ColumnValue columnValue : ll) {
				System.out.println("rowkey: "+generalTableInfo.getRowkey()+"  "+"family: "+
			columnValue.getFamily()+"column: "+columnValue.getColumn()+"value: "+columnValue.getValue());
			}
		}
//		hApi.deleteData("nfmedia_test", "1","nfmedia","age"); 
//		String[] aa = {
//			"1",
//			"99",
//			"199"
//		};
//		Map<String, Map<String, String>> result = hApi.getDatas("nfmedia_test", aa, "nfmedia");
//		System.out.println("批量获取rowkey数组中列簇为nfmedia的所有数据");
//		for (String string : result.keySet()) {
//			Map<String, String> ss = result.get(string);
//			for(String str : ss.keySet()){
//				System.out.println("rowkey:" + string + "  列簇:列名 " + str + " => " + ss.get(str));
//			}
//		}
//		System.out.println("批量获取rowkey数组中版本数为3的所有数据");
//		Map<String, Map<String, List<String>>> result1 = hApi.getDatas("nfmedia_test", aa,3);
//		for (String string : result1.keySet()) {
//			Map<String, List<String>> ss = result1.get(string);
//			for(String str : ss.keySet()){
//				List<String> sss = ss.get(str);
//				for (String string2 : sss) {
//					System.out.println("rowkey:" + string + "  列簇:列名 " + str + " => " + string2);
//				}
//				
//			}
//		}
//		System.out.println("批量获取rowkey数组中列簇为nfmedia且版本数为3的所有数据");
//		Map<String, Map<String, List<String>>> result3 = hApi.getDatas("nfmedia_test", aa, "nfmedia",3);
//		for (String string : result3.keySet()) {
//			Map<String, List<String>> ss = result3.get(string);
//			for(String str : ss.keySet()){
//				List<String> sss = ss.get(str);
//				for (String string2 : sss) {
//					System.out.println("rowkey:" + string + "  列簇:列名 " + str + " => " + string2);
//				}
//				
//			}
//		}
//		Map<String, String> result = hApi.getData("nfmedia_test", "1", "nfmedia");
//		for(String str:result.keySet()){
//			System.out.println(str + "   " + result.get(str));
//		}
//		List<GeneralTableInfo> generalTableInfos = new ArrayList<GeneralTableInfo>();
//		for(int i=0;i<10000;i++){
//			List<ColumnValue> columnValues = new ArrayList<ColumnValue>();
//			columnValues.add(new ColumnValue("nfmedia", "job", "南方报业传媒集团"));
//			columnValues.add(new ColumnValue("nfmedia", "address", "广州大道中289号"));
//			GeneralTableInfo geInfo = new GeneralTableInfo(""+i, columnValues);
//			generalTableInfos.add(geInfo);
//		}
//		
//		hApi.putDatas("nfmedia_test", generalTableInfos);
	}
}
