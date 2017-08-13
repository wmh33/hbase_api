package api;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import utils.HbaseConfUtil;

public class HbaseInit {
	public static Admin admin;
	public static Connection conn;
	public HbaseInit() {
		try {
			conn = HbaseConfUtil.getConn();
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
