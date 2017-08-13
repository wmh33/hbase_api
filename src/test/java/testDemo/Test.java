package testDemo;

import java.util.ArrayList;
import java.util.List;

public class Test {
public static void main(String[] args) {
	List<Long> m = new ArrayList<Long>();
	long i = 0;
	while(true){
		m.add(i);
		i++;
		System.out.println(i);
	}
}
}
