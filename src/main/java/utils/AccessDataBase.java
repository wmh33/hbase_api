package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AccessDataBase {
	public static String URL;
	public static String USERNAME;
	public static String PASSWORD;
	public static final String CLAZZ = "com.mysql.jdbc.Driver";
	/***
	 * 获取数据库连接
	 * @return conn
	 */
	public static Connection getConnection(){
		Connection conn = null;
		try {
			Class.forName(CLAZZ);
			conn =  DriverManager.getConnection(URL,USERNAME,PASSWORD);
			System.out.println("数据库连接成功！");
		} catch (ClassNotFoundException e) {
			System.err.println("mysql驱动不存在");
		}catch (SQLException e) {
			System.err.println("连接失败！！！");
			e.printStackTrace();
		}		
		return conn;	
	}
	/***
	 * 关闭数据库连接
	 * @param rs
	 * @param st
	 * @param conn
	 */
	public static void closeConn(ResultSet rs,Statement st,Connection conn){
		try {
			if (rs != null) {
				rs.close();
			} 
		}catch (SQLException e) {
				e.printStackTrace();
		}
		try {
			if (st != null) {
				st.close();
			} 
		}catch (SQLException e) {
				e.printStackTrace();
		}
		try {
			if (conn != null){
				conn.close();
			} 
		} catch (SQLException e) {
				e.printStackTrace();
		}
	}	
}
