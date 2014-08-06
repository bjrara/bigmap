package com.ctriposs.bigmap.page;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMappedPageFactory implements IMappedPageFactory {

	private final static Logger logger = LoggerFactory.getLogger(AbstractMappedPageFactory.class);

	protected static final String PAGE_FILE_NAME = "page";
	protected static final String PAGE_FILE_TYPE = ".dat";

	protected final int pageSize;
	protected final String pageFileName;
	protected final String pageDir;

	protected boolean deleted;
	protected File pageFolder;

	public AbstractMappedPageFactory(int pageSize, String pageDir) {
		this.pageFolder = new File(pageDir);
		if (!pageDir.endsWith(File.separator)) {
			pageDir += File.separator;
		}
		this.pageDir = pageDir;
		this.pageSize = pageSize;
		this.pageFileName = this.pageDir + PAGE_FILE_NAME + "-";

		if (!pageFolder.exists()) {
			pageFolder.mkdirs();
		}
	}

	abstract protected void clearCache() throws IOException;

	abstract protected void cacheRemovePage(long index) throws IOException;

	abstract protected long getIndexByFileName(String fileName);

	abstract protected String getFileNameByIndex(long index);

	@Override
	public int getPageSize() {
		return pageSize;
	}

	@Override
	public String getPageDir() {
		return pageDir;
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
		this.deletePages(indexSet);
		if (logger.isDebugEnabled()) {
			logger.debug("All page files in dir " + pageDir + " have been deleted.");
		}
	}

	@Override
	public void releaseCachedPages() throws IOException {
		this.clearCache();
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
				long index = this.getIndexByFileName(fileName);
				indexSet.add(index);
			}
		}
		return indexSet;
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
