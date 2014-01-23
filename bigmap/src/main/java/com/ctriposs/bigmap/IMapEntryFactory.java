package com.ctriposs.bigmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.NavigableSet;

/**
 * A Factory managing the creation & recycle of the map entry
 * 
 * @author bulldog
 *
 */
public interface IMapEntryFactory extends Closeable {
	
	/**
	 * Acquire a new map entry, either new or reused
	 * 
	 * @param length length of the slot
	 * @return a map entry
	 * @throws IOException exception throw during the acquire operation
	 */
	public MapEntry acquire(int length) throws IOException;
	
	/**
	 * Release a map entry into the pool
	 * 
	 * @param me map entry
	 * @throws IOException exception thrown during the release operation
	 */
	public void release(MapEntry me) throws IOException;
	
	/**
	 * Find a map entry by specified index
	 * 
	 * @param index the target index
	 * @return a map entry
	 * @throws IOException exception thrown during the finding operation
	 */
	public MapEntry findMapEntryByIndex(long index) throws IOException;
	
	/**
     * Remove all data in the pool, this will empty the map and delete all back page files.
     *
     */
	public void removeAll() throws IOException;
	
	/**
     * Get total size of back files(index and data files) of the big map
     *
     * @return total size of back files
     * @throws IOException exception thrown if there was any IO error during the getBackFileSize operation
     */
    long getBackFileSize() throws IOException;
    
    /**
     * Get total number of free entries
     * 
     * @return total number of free entries
     */
    long getFreeEntryCount();
    
    /**
     * Get total number of free entries with specific length
     * 
     * @param length target length
     * @return total number of free entries
     */
    long getFreeEntryCountByLength(int length);
    
    /**
     * Get total number of allocated(free + used) entries
     * 
     * @return total number of entries
     */
    long getTotalEntryCount();
    
    /**
     * Get total free slot size with specific length
     * 
     * @param length target length
     * @return total free slot size
     */
    long getTotalFreeSlotSizeByLength(int length);
    
    /**
     * Get total free slot size
     * 
     * @return total free slot size
     */
    long getTotalFreeSlotSize();
    
    /**
     * Get total slot size allocated(free + used)
     * 
     * @return total slot size
     */
    long getTotalSlotSize();
    
    /**
     * Get currently used total slot size
     * 
     * @return total slot size
     */
    long getTotalUsedSlotSize();

    /**
     * Get total really used slot size
     * 
     * @return total slot size
     */
    long getTotalRealUsedSlotSize();
    
    /**
     * Get currently wasted total slot size
     * 
     * @return total slot size
     */
    long getTotalWastedSlotSize();
    
    /**
     * For testing only
     * 
     * @return
     */
    NavigableSet<Integer> getFreeEntryIndexSet();
    
    /**
     * Total number of acquire counter
     * 
     * @return counter
     */
    long getTotalAcquireCounter();
    
    /**
     * Total number of release counter
     * 
     * @return counter
     */
    long getTotalReleaseCounter();
    
    /**
     * Total number of exact match reuse counter
     * 
     * @return counter
     */
    long getTotalExatchMatchReuseCounter();
    
    /**
     * Total number of approximate match reuse counter
     * 
     * @return counter
     */
    long getTotalApproximateMatchReuseCounter();
    
    /**
     * Total number of acquire new counter
     * 
     * @return counter
     */
    long getTotalAcquireNewCounter();
}
