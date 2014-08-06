package com.ctriposs.bigmap.filemap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctriposs.bigmap.page.AbstractMappedPageFactory;
import com.ctriposs.bigmap.page.IMappedPage;
import com.ctriposs.bigmap.page.IMappedPageFactory;
import com.ctriposs.bigmap.utils.FileUtil;

public class FileMappedPageFactory extends AbstractMappedPageFactory implements IMappedPageFactory {

	private final static Logger logger = LoggerFactory.getLogger(FileMappedPageFactory.class);

	private final Map<Long, FileMappedPage> cache;

	public FileMappedPageFactory(int pageSize, String pageDir) {
		super(pageSize, pageDir);

		this.cache = new HashMap<Long, FileMappedPage>();

	}

	@Override
	public IMappedPage acquirePage(long index) throws IOException {
		FileMappedPage fmp = cache.get(index);
		if (fmp == null) {
			synchronized (cache) {
				fmp = cache.get(index);
				if (fmp == null) {
					String fileName = getFileNameByIndex(index);
					RandomAccessFile raf = null;
					try {
						raf = new RandomAccessFile(fileName, "rw");
//					if (raf.length() <= 0) {
//						raf.setLength(INIT_DATA_FILE_SIZE);
//					}
						//TODO channel needs to be closed?
						FileChannel channel = raf.getChannel();
						MappedByteBuffer byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, pageSize);
						fmp = new FileMappedPage(index, byteBuffer, raf, channel, fileName);
						cache.put(index, fmp);
						if (logger.isDebugEnabled()) {
							logger.debug("Mapped page for " + fileName + " was just created and cached.");
						}
					} finally {

					}

				} else {
					if (logger.isDebugEnabled())
						logger.debug("Hit mapped page " + fmp.getPageFile() + " in cache.");
				}
			}
		}

		return fmp;
	}

	@Override
	public void deletePage(long index) throws IOException {
		this.cacheRemovePage(index);
		String fileName = this.getFileNameByIndex(index);
		int round = 0;
		int maxRound = 10;
		while (round < maxRound) {
			try {
				FileUtil.deleteFile(new File(fileName));
				deleted = true;
			} catch (IllegalStateException ex) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
				}
				round++;
				if (logger.isDebugEnabled()) {
					logger.warn("fail to delete file " + fileName + ", tried round = " + round);
				}
			}
		}
		if (deleted) {
			logger.info("Page file " + fileName + " was just deleted.");
		} else {
			logger.warn("fail to delete file " + fileName + " after max " + maxRound
					+ " rounds of try, you may delete it manually.");
		}
	}

	@Override
	public Set<Long> getPageIndexSetBefore(long timestamp) {
		Set<Long> indexSetHist = new HashSet<Long>();
		File[] pageFiles = pageFolder.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for (File pageFile : pageFiles) {
				if (pageFile.lastModified() < timestamp) {
					String fileName = pageFile.getName();
					if (fileName.endsWith(PAGE_FILE_TYPE)) {
						long index = this.getIndexByFileName(fileName);
						indexSetHist.add(index);
					}
				}
			}
		}
		return indexSetHist;
	}

	@Override
	public int getCacheSize() {
		return cache.size();
	}

	@Override
	public void flush() {
		Collection<FileMappedPage> cachePages = cache.values();
		for (FileMappedPage mappedPage : cachePages) {
			mappedPage.flush();
		}
	}

	@Override
	protected void clearCache() throws IOException {
		for (FileMappedPage fmp : cache.values()) {
			fmp.close();
		}
		cache.clear();
	}

	@Override
	protected void cacheRemovePage(long index) throws IOException {
		FileMappedPage fmp = cache.remove(index);
		if (fmp != null) {
			fmp.close();
		}

	}

	@Override
	protected long getIndexByFileName(String fileName) {
		int beginIndex = fileName.lastIndexOf('-');
		beginIndex += 1;
		int endIndex = fileName.lastIndexOf(PAGE_FILE_TYPE);
		String sIndex = fileName.substring(beginIndex, endIndex);
		long index = Long.parseLong(sIndex);
		return index;
	}

	@Override
	protected String getFileNameByIndex(long index) {
		return String.format("%s%d%s", pageFileName, index, PAGE_FILE_TYPE);
	}
}
