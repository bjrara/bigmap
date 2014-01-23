package com.ctriposs.bigmap;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.bigmap.utils.FileUtil;

public class LimitTest {

	private static String testDir = TestUtil.TEST_BASE_DIR + "bigmap/unit/perf_test";
	
	private BigConcurrentHashMapImpl map;
	
	@Test
	public void limitTest() throws IOException, InterruptedException {
		map = new BigConcurrentHashMapImpl(testDir, "limitTest");
		
		/*
		for(long counter = 0;; counter++) {
			map.put(String.valueOf(counter).getBytes(), "a".getBytes());
			if (counter%1000000 == 0) System.out.println(""+counter);
		}
		*/
	}
	
	@After
	public void clear() throws IOException {
		if (map != null) {
			map.close();
		}
		FileUtil.deleteDirectory(new File(testDir));
	}

}
