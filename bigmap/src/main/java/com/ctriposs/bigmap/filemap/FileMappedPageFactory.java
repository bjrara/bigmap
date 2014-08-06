package com.ctriposs.bigmap.filemap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctriposs.bigmap.page.IMappedPage;
import com.ctriposs.bigmap.page.IMappedPageFactory;
import com.ctriposs.bigmap.utils.FileUtil;

public class FileMappedPageFactory implements IMappedPageFactory {

	private final static Logger logger = LoggerFactory.getLogger(FileMappedPageFactory.class);

	private static final String PAGE_FILE_NAME = "page";
	private static final String PAGE_FILE_TYPE = "dat";

	private final Map<Long, FileMappedPage> cache;

	private final int pageSize;
	private final String pageFileName;
	private final String pageDir;

	private boolean deleted;
	private File pageFolder;

	public FileMappedPageFactory(int pageSize, String pageDir) {
		if (!pageDir.endsWith(File.separator)) {
			pageDir += File.separator;
		}
		this.pageDir = pageDir;
		this.pageSize = pageSize;

		this.pageFileName = this.pageDir + PAGE_FILE_NAME + "-";
		this.cache = new HashMap<Long, FileMappedPage>();

		this.pageFolder = new File(this.pageDir);
		if (!pageFolder.exists()) {
			pageFolder.mkdirs();
		}
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
						fmp = new FileMappedPage(index, byteBuffer,raf, channel, fileName);
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

	private String getFileNameByIndex(long index) {
		return String.format("%s%d.%s", pageFileName, index, PAGE_FILE_TYPE);
	}

	@Override
	public int getPageSize() {
		return pageSize;
	}

	@Override
	public String getPageDir() {
		return pageDir;
	}

	@Override
	public void deletePage(long index) {
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

	private void cacheRemovePage(long index) {
		FileMappedPage fmp = cache.remove(index);
		if (fmp != null) {
			fmp.close();
		}

	}

	private void clearCache() {
		for (FileMappedPage fmp : cache.values()) {
			fmp.close();
		}
		cache.clear();
	}

	@Override
	public void deletePages(Set<Long> indexes) throws IOException {
		if (indexes == null)
			return;
		for (long index : indexes) {
			this.deletePage(index);
		}
	}

	@Override
	public void deleteAllPages() throws IOException {
		this.clearCache();
		Set<Long> indexSet = this.getExistingBackFileIndexSet();
		deletePages(indexSet);
		if (logger.isDebugEnabled()) {
			logger.debug("All page files in dir " + pageDir + " have been deleted.");
		}
	}

	@Override
	public void releaseCachedPages() throws IOException {
		this.clearCache();
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
	public void deletePagesBefore(long timestamp) throws IOException {
		Set<Long> indexSetHist = this.getPageIndexSetBefore(timestamp);
		this.deletePages(indexSetHist);
		if (logger.isDebugEnabled()) {
			logger.debug("All page files in dir [" + this.pageDir + "], before [" + timestamp + "] have been deleted.");
		}
	}

	@Override
	public long getPageFileLastModifiedTime(long index) {
		String pageFileName = this.getFileNameByIndex(index);
		File pageFile = new File(pageFileName);
		if (!pageFile.exists())
			return -1L;
		return pageFile.lastModified();
	}

	@Override
	public long getFirstPageIndexBefore(long timestamp) {
		Set<Long> indexSetHist = this.getPageIndexSetBefore(timestamp);
		if (indexSetHist.size() == 0)
			return -1L;
		TreeSet<Long> sortedIndexSet = new TreeSet<Long>(indexSetHist);
		Long largestIndex = sortedIndexSet.last();
		if (largestIndex != Long.MAX_VALUE) {
			return largestIndex;
		} else {
			Long next = 0L;
			while (sortedIndexSet.contains(next)) {
				next++;
			}
			if (next == 0L) {
				return Long.MAX_VALUE;
			} else {
				return --next;
			}
		}
	}

	@Override
	public Set<Long> getExistingBackFileIndexSet() {
		Set<Long> indexSet = new HashSet<Long>();
		File[] pageFiles = pageFolder.listFiles();
		for (File pageFile : pageFiles) {
			String fileName = pageFile.getName();
			if (fileName.endsWith(PAGE_FILE_TYPE)) {
				long index = getIndexByFileName(fileName);
				indexSet.add(index);
			}
		}
		return indexSet;
	}

	private long getIndexByFileName(String fileName) {
		int beginIndex = fileName.lastIndexOf('-');
		beginIndex += 1;
		int endIndex = fileName.lastIndexOf("."+PAGE_FILE_TYPE);
		String sIndex = fileName.substring(beginIndex, endIndex);
		long index = Long.parseLong(sIndex);
		return index;
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
	public Set<String> getBackPageFileSet() {
		Set<String> fileSet = new HashSet<String>();
		File[] pageFiles = pageFolder.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for (File pageFile : pageFiles) {
				String fileName = pageFile.getName();
				if (fileName.endsWith(PAGE_FILE_TYPE)) {
					fileSet.add(fileName);
				}
			}
		}
		return fileSet;
	}

	@Override
	public long getBackPageFileSize() {
		long totalSize = 0L;
		File[] pageFiles = pageFolder.listFiles();
		if (pageFiles != null && pageFiles.length > 0) {
			for (File pageFile : pageFiles) {
				String fileName = pageFile.getName();
				if (fileName.endsWith(PAGE_FILE_TYPE)) {
					totalSize += pageFile.length();
				}
			}
		}
		return totalSize;
	}

}
