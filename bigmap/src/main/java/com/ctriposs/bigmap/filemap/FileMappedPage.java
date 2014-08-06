package com.ctriposs.bigmap.filemap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctriposs.bigmap.page.IMappedPage;

public class FileMappedPage implements IMappedPage {

	private final static Logger logger = LoggerFactory.getLogger(FileMappedPage.class);
	private final long index;
	private final FileChannel dataChannel;

	private boolean close = false;
	private boolean dirty = false;
	private String pageFile;
	private MappedByteBuffer buffer;
	private RandomAccessFile file;

	public FileMappedPage(long index, MappedByteBuffer buffer, FileChannel channel, String pageFile) {
		this.index = index;
		this.buffer = buffer;
		this.dataChannel = channel;
		this.pageFile = pageFile;
	}

	public FileMappedPage(long index, MappedByteBuffer buffer, RandomAccessFile raf, FileChannel channel,
			String pageFile) {
		this.index = index;
		this.buffer = buffer;
		this.dataChannel = channel;
		this.pageFile = pageFile;
		this.file = raf;
	}

	public void close() {
		synchronized (this) {
			if (close)
				return;
			flush();

			unmap(buffer);

			buffer = null;
			try {
				file.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			close = true;
		}
	}

	@Override
	public ByteBuffer getLocal(int position) {
		ByteBuffer buf = this.getLocal();
		buf.position(position);
		return buf;
	}

	@Override
	public ByteBuffer getLocal() {
		return buffer;
	}

	@Override
	public byte[] getLocal(int position, int length) {
		byte[] data = new byte[length];
		ByteBuffer buf = this.getLocal();
		buf.position(position);
		buf.get(data);
		return data;
	}

	@Override
	public boolean isClosed() {
		return close;
	}

	@Override
	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	@Override
	public String getPageFile() {
		return pageFile;
	}

	@Override
	public long getPageIndex() {
		return index;
	}

	@Override
	public void flush() {
		try {
			if (dirty) {
				dataChannel.write(buffer);
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Fail to write file: " + pageFile);
		}
	}

	public void flush(int position) {
		try {
			dataChannel.write(buffer, position);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Fail to write file: " + pageFile);
		}
	}

	private static void unmap(MappedByteBuffer buffer)
	{
		Cleaner.clean(buffer);
	}

	/**
	 * Helper class allowing to clean direct buffers.
	 */
	private static class Cleaner {
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

}
