package com.ctriposs.bigmap;

import java.io.Closeable;
import java.io.IOException;

public interface IBigConcurrentHashMap extends Closeable {
	/**
	 * The size of back file used
     * @throws IOException exception throws during file IO operation 
	 * @return used back file size
	 */
	long BackFileUsed() throws IOException;
	
    /**
     * Removes all mappings from this hash map, leaving it empty.
     *
     * @see #isEmpty
     * @see #size
     */
	public void clear();
	
    /**
     * Returns the value of the mapping with the specified key.
     *
     * @param key the key.
     * @throws IOException exception throws during file IO operation 
     * @return the value of the mapping with the specified key, or {@code null}
     *         if no mapping for the specified key is found.
     */
	public byte[] get(byte[] key) throws IOException;
	
    /**
     * Returns whether this map is empty.
     *
     * @return {@code true} if this map has no elements, {@code false}
     *         otherwise.
     * @see #size()
     */
	public boolean isEmpty();
	
    /**
     * Maps the specified key to the specified value.
     *
     * @param key   the key.
     * @param value the value.
     * @throws IOException exception throws during file IO operation
     * @return the value of any previous mapping with the specified key or
     *         {@code null} if there was no such mapping.
     */
	public byte[] put(byte[] key, byte[] value) throws IOException;
	
    /**
     * Removes the mapping from this map
     *
     * @param key to remove
     * @throws IOException exception throws during file IO operation
     *  @return value contained under this key, or null if value did not exist
     */
	public byte[] remove(byte[] key) throws IOException;
	
    /**
     * Returns the number of elements in this map.
     *
     * @return the number of elements in this map.
     */
	public int size();
	
	/**
	 * remove all data in the map, including backing file.
	 * 
	 * @throws IOException exception throws during file IO operation.
	 */
	public void removeAll() throws IOException;
}
