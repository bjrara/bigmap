package com.ctriposs.bigmap.page;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;

/**
 * Helper class allowing to clean direct buffers.
 */
public class Cleaner {
	public static final boolean CLEAN_SUPPORTED;
	private static final Method directBufferCleaner;
	private static final Method directBufferCleanerClean;

	static {
		Method directBufferCleanerX = null;
		Method directBufferCleanerCleanX = null;
		boolean v;
		try {
			directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
			directBufferCleanerX.setAccessible(true);
			directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
			directBufferCleanerCleanX.setAccessible(true);
			v = true;
		} catch (Exception e) {
			v = false;
		}
		CLEAN_SUPPORTED = v;
		directBufferCleaner = directBufferCleanerX;
		directBufferCleanerClean = directBufferCleanerCleanX;
	}

	public static void clean(ByteBuffer buffer) {
		if (buffer == null)
			return;
		if (CLEAN_SUPPORTED && buffer.isDirect()) {
			try {
				Object cleaner = directBufferCleaner.invoke(buffer);
				directBufferCleanerClean.invoke(cleaner);
			} catch (Exception e) {
				// silently ignore exception
			}
		}
	}
}
