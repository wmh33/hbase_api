package utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConfUtil {
	/***
	 * @return connection => 返回hbase的connection
	 * @throws IOException
	 */
	public static Connection getConn() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "master:16000");// 指定HMaster
		conf.set("hbase.rootdir", "hdfs://master:8020/hbase");// 指定HBase在HDFS上存储路径
		conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");// 指定使用的Zookeeper集群
		conf.set("hbase.zookeeper.property.clientPort", "2181");// 指定使用Zookeeper集群的端口
		Connection connection = ConnectionFactory.createConnection(conf);// 获取连接
		return connection;
	}
}
