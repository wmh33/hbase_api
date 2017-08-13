package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import mapper.HBaseToHBaseMapper;
import utils.JarUtil;

public class HBaseDriver extends Configured implements Tool{
	private String fromTable=""; //导入表
	private String toTable="";  //导出表
	private String setVersion=""; //是否设置版本
	// args => {FromTable,ToTable,SetVersion,ColumnFromTable,ColumnToTable}
	public int run(String[] args) throws Exception {
		if(args.length!=5){
			System.err.println("Usage:\n hbaseDriver.HbaseDriver <input> <fromTableName> "
					+ "<output> <toTableName>"
					+"< versions >"
					+ " <cf1:c1,cf1:c2,cf1:c10,cf1:c11,cf1:c14> or <-1> "
					+ "<cf1:c1,cf1:c10,cf1:c14> or <-1>");
			return -1;
		}
		Configuration conf = getConfiguration();
		fromTable = args[0];
		toTable = args[1];
		setVersion = args[2];
		conf.set("SETVERSION", setVersion);
		if(!args[3].equals("-1")){
			conf.set("COLUMNFROMTABLE", args[3]);
		}
		if(!args[4].equals("-1")){
			conf.set("COLUMNTOTABLE", args[4]);
		}
		String jobName ="From table "+fromTable+ " ,Import to "+ toTable;
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(HBaseDriver.class);
		Scan scan = new Scan();
		// 判断是否需要设置版本
		if(Integer.parseInt(setVersion) > 1){
			scan.setMaxVersions(Integer.parseInt(setVersion));
		}
		// 设置HBase表输入：表名、scan、Mapper类、mapper输出键类型、mapper输出值类型
		TableMapReduceUtil.initTableMapperJob(
				fromTable, 
				scan, 
				HBaseToHBaseMapper.class, 
				ImmutableBytesWritable.class, 
				Put.class, 
				job);
		// 设置HBase表输出：表名，reducer类
		TableMapReduceUtil.initTableReducerJob(toTable, null, job);
		// 没有 reducers，  直接写入到 输出文件
	    job.setNumReduceTasks(0);
	 
        return job.waitForCompletion(true) ? 0 : 1;
        
	}
	private static Configuration configuration;
	public static Configuration getConfiguration(){
		if(configuration==null){
			/**
			 * 直接从Windows提交代码到Hadoop集群
			 */
			configuration = new Configuration();
			configuration.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
			configuration.set("fs.defaultFS", "hdfs://master:8020");// 指定namenode
			configuration.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
			configuration.set("yarn.resourcemanager.address", "master:8032"); // 指定resourcemanager
			configuration.set("yarn.resourcemanager.scheduler.address", "master:8030");// 指定资源分配器
			configuration.set("mapreduce.jobhistory.address", "master:10020");// 指定historyserver
			configuration.set("hbase.master", "master:16000");
			configuration.set("hbase.rootdir", "hdfs://master:8020/hbase");
			configuration.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
			configuration.set("hbase.zookeeper.property.clientPort", "2181");
			// 需export->jar file ; 设置正确的jar包所在位置
			configuration.set("mapreduce.job.jar",JarUtil.jar(HBaseDriver.class));// 设置jar包路径
		}
		
		return configuration;
	}

}
