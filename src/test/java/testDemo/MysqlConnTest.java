package testDemo;

import utils.AccessDataBase;

public class MysqlConnTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AccessDataBase.URL = "jdbc:mysql://master:3306/nfmedia?characterEncoding=UTF-8";
		AccessDataBase.USERNAME = "root";
		AccessDataBase.PASSWORD = "root";
		AccessDataBase.getConnection();
	}

}
