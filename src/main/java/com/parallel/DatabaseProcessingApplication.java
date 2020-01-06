package com.parallel;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class DatabaseProcessingApplication {

	public static void main(String[] args) {
		int count = 8;
		Integer[] r = new DatabaseProcessingApplication().getCpuCountSteps(count);
		for (Integer integer : r) {
			System.out.println(integer);
		}
		SpringApplication.run(DatabaseProcessingApplication.class, args);
	}

	public Integer[] getCpuCountSteps(int no) {
		if (!isBinaryNumber(no)) {
			return null;
		}
		List<Integer> result = new ArrayList<>();
		result.add(no);
		while (no != 1) {
			result.add(no / 2);
			no = no / 2;
		}
		return result.toArray(new Integer[result.size()]);
	}

	public boolean isBinaryNumber(int no) {
		return Integer.toBinaryString(no - 1).replace("1", "").isEmpty();
	}


}
