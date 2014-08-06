package com.ctriposs.bigmap;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Segments are specialized versions of hash tables.  This
 * subclasses from ReentrantLock opportunistically, just to
 * simplify some locking and avoid separate construction.
 */
class Segment<V> extends ReentrantLock implements Serializable {

	

	private static final long serialVersionUID = 2249069246763182397L;

	/**
	 * The number of elements in this segment's region.
	 */
	transient volatile int count;

	/**
	 * The table is rehashed when its size exceeds this threshold.
	 * (The value of this field is always <tt>(int)(capacity *
	 * loadFactor)</tt>.)
	 */
	transient int threshold;

	/**
	 * The per-segment table.
	 */
	transient volatile HashEntry[] table;

	/**
	 * The load factor for the hash table.  Even though this value
	 * is same for all segments, it is replicated to avoid needing
	 * links to outer object.
	 * @serial
	 */
	final float loadFactor;

	/**
	 * Factory managing the creation, recycle/reuse of map entries mapped to disk files.
	 */
	final IMapEntryFactory mapEntryFactory;

	Segment(int initialCapacity, float lf, IMapEntryFactory mapEntryFactory) {
		super(false);
		loadFactor = lf;
		this.mapEntryFactory = mapEntryFactory;
		setTable(HashEntry.newArray(initialCapacity));
	}

	@SuppressWarnings("unchecked")
	static <V> Segment<V>[] newArray(int i) {
		return new Segment[i];
	}

	/**
	 * Sets table to new HashEntry array.
	 * Call only while holding lock or in constructor.
	 */
	void setTable(HashEntry[] newTable) {
		threshold = (int) (newTable.length * loadFactor);
		table = newTable;
	}

	/**
	 * Returns properly casted first entry of bin for given hash.
	 */
	HashEntry getFirst(int hash) {
		HashEntry[] tab = table;
		return tab[hash & (tab.length - 1)];
	}

	/* Specialized implementations of map methods */

	byte[] get(final byte[] key, int hash) throws IOException {
		if (count != 0) { // read-volatile
			lock();
			try {
				int c = count - 1;
				HashEntry[] tab = table;
				int index = hash & (tab.length - 1);
				HashEntry e = tab[index];
				while (e != null) {
					MapEntry me = this.mapEntryFactory.findMapEntryByIndex(e.index);
					if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {

						if (this.isExpired(me)) {
							this.mapEntryFactory.release(me);

							this.removeEntry(tab, index, e);

							count = c; // write-volatile

							return null;
						} else {
							me.putLastAccessedTime(System.currentTimeMillis());
							return me.getEntryValue();
						}
					}
					e = e.next;
				}
			} finally {
				unlock();
			}
		}
		return null;
	}

	void removeEntry(HashEntry[] tab, int index, HashEntry e) {
		HashEntry first = tab[index];

		if (first == e) {
			tab[index] = e.next;
			e.next = null; // ready for GC
		} else {
			HashEntry p = first;
			while (p.next != e) {
				p = p.next;
			}
			p.next = e.next;
			e.next = null; // ready for GC
		}
	}

	boolean containsKey(final byte[] key, int hash) throws IOException {
		if (count != 0) { // read-volatile
			lock();
			try {
				int c = count - 1;
				HashEntry[] tab = table;
				int index = hash & (tab.length - 1);
				HashEntry e = tab[index];
				while (e != null) {
					MapEntry me = this.mapEntryFactory.findMapEntryByIndex(e.index);
					if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {

						if (this.isExpired(me)) {
							this.mapEntryFactory.release(me);

							this.removeEntry(tab, index, e);

							count = c; // write-volatile

							return false;
						} else {
							me.putLastAccessedTime(System.currentTimeMillis());
							return true;
						}

					}
					e = e.next;
				}
			} finally {
				unlock();
			}
		}
		return false;
	}

	boolean replace(byte[] key, int hash, byte[] oldValue, byte[] newValue, long ttlInMs) throws IOException {
		lock();
		try {
			HashEntry e = getFirst(hash);
			MapEntry me = null;
			while (e != null) {
				me = this.mapEntryFactory.findMapEntryByIndex(e.index);

				if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {
					break;
				}

				e = e.next;
			}

			boolean replaced = false;
			if (e != null && Arrays.equals(oldValue, me.getEntryValue())) {
				replaced = true;
				this.mapEntryFactory.release(me);
				me = this.mapEntryFactory.acquire(key.length + newValue.length);
				me.putKeyLength(key.length);
				me.putValueLength(newValue.length);
				me.putEntryKey(key);
				me.putEntryValue(newValue);
				me.putLastAccessedTime(System.currentTimeMillis());
				me.putTimeToLive(ttlInMs);

				e.index = me.getIndex();
			}
			return replaced;
		} finally {
			unlock();
		}
	}

	byte[] replace(byte[] key, int hash, byte[] newValue, long ttlInMs) throws IOException {
		lock();
		try {
			HashEntry e = getFirst(hash);
			MapEntry me = null;
			while (e != null) {
				me = this.mapEntryFactory.findMapEntryByIndex(e.index);

				if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {
					break;
				}

				e = e.next;
			}

			byte[] oldValue = null;
			if (e != null) {
				oldValue = me.getEntryValue();
				this.mapEntryFactory.release(me);
				me = this.mapEntryFactory.acquire(key.length + newValue.length);
				me.putKeyLength(key.length);
				me.putValueLength(newValue.length);
				me.putEntryKey(key);
				me.putEntryValue(newValue);
				me.putLastAccessedTime(System.currentTimeMillis());
				me.putTimeToLive(ttlInMs);

				e.index = me.getIndex();
			}
			return oldValue;
		} finally {
			unlock();
		}
	}

	void restoreInUseMapEntry(MapEntry me, int hash) throws IOException {
		lock();
		try {
			int c = count;
			if (c++ > threshold) // ensure capacity
				rehash();
			HashEntry[] tab = table;
			int index = hash & (tab.length - 1);
			HashEntry first = tab[index];

			tab[index] = new HashEntry(me.getIndex(), hash, first);
			count = c; // write-volatile
		} finally {
			unlock();
		}
	}

	byte[] put(byte[] key, int hash, byte[] value, boolean onlyIfAbsent, long ttlInMs) throws IOException {
		lock();
		try {
			int c = count;
			if (c++ > threshold) // ensure capacity
				rehash();
			HashEntry[] tab = table;
			int index = hash & (tab.length - 1);
			HashEntry first = tab[index];
			HashEntry e = first;
			MapEntry me = null;
			while (e != null) {
				me = this.mapEntryFactory.findMapEntryByIndex(e.index);

				if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {
					break;
				}

				e = e.next;
			}

			byte[] oldValue;
			if (e != null) {
				oldValue = me.getEntryValue();
				if (!onlyIfAbsent) {
					this.mapEntryFactory.release(me);
					me = this.mapEntryFactory.acquire(key.length + value.length);
					me.putKeyLength(key.length);
					me.putValueLength(value.length);
					me.putEntryKey(key);
					me.putEntryValue(value);
					me.putLastAccessedTime(System.currentTimeMillis());
					me.putTimeToLive(ttlInMs);

					e.index = me.getIndex();
				}
			}
			else {
				oldValue = null;

				me = this.mapEntryFactory.acquire(key.length + value.length);
				me.putKeyLength(key.length);
				me.putValueLength(value.length);
				me.putEntryKey(key);
				me.putEntryValue(value);
				me.putLastAccessedTime(System.currentTimeMillis());
				me.putTimeToLive(ttlInMs);

				tab[index] = new HashEntry(me.getIndex(), hash, first);
				count = c; // write-volatile
			}
			return oldValue;
		} finally {
			unlock();
		}
	}

	void rehash() {
		HashEntry[] oldTable = table;
		int oldCapacity = oldTable.length;
		if (oldCapacity >= BigConcurrentHashMapImpl.MAXIMUM_CAPACITY)
			return;

		HashEntry[] newTable = HashEntry.newArray(oldCapacity << 1);
		threshold = (int) (newTable.length * loadFactor);
		int sizeMask = newTable.length - 1;
		for (HashEntry e : oldTable) {
			if (e != null) {
				HashEntry p = e;
				HashEntry q = e.next;

				while (true) {
					int k = p.hash & sizeMask;
					p.next = newTable[k];
					newTable[k] = p;

					p = q;
					if (p == null)
						break;
					else {
						q = p.next;
					}
				}
			}
		}
		table = newTable;
	}

	// Purge expired entries
	void purge() throws IOException {
		if (count != 0) {
			lock();
			try {
				for (int index = 0; index < table.length; index++) {
					HashEntry e = table[index];
					while (e != null) {
						MapEntry me = this.mapEntryFactory.findMapEntryByIndex(e.index);

						HashEntry next = e.next;
						if (this.isExpired(me)) {
							this.mapEntryFactory.release(me);

							this.removeEntry(table, index, e);

							count--;
						}

						e = next;
					}
				}
			} finally {
				unlock();
			}
		}
	}

	/**
	 * Remove; match on key only if value null, else match both.
	 * @throws IOException 
	 */
	byte[] remove(final byte[] key, int hash, byte[] value) throws IOException {
		lock();
		try {
			int c = count - 1;
			HashEntry[] tab = table;
			int index = hash & (tab.length - 1);
			HashEntry e = tab[index];
			MapEntry me = null;
			while (e != null) {
				me = this.mapEntryFactory.findMapEntryByIndex(e.index);

				if (e.hash == hash && Arrays.equals(key, me.getEntryKey())) {
					break;
				}

				e = e.next;
			}

			byte[] oldValue = null;
			if (e != null) {
				if (value == null || Arrays.equals(value, me.getEntryValue())) {
					oldValue = me.getEntryValue();

					if (this.isExpired(me)) {
						oldValue = null;
					}

					this.mapEntryFactory.release(me);

					this.removeEntry(tab, index, e);

					count = c; // write-volatile
				}
			}
			return oldValue;
		} finally {
			unlock();
		}
	}

	boolean isExpired(MapEntry me) {
		// has the entry expired?
		long ttlInMs = me.getTimeToLive();
		boolean expired = ttlInMs > 0 && (System.currentTimeMillis() - me.getLastAccessedTime() > ttlInMs);
		return expired;
	}

	void clear() {
		if (count != 0) {
			lock();
			try {
				HashEntry[] tab = table;
				for (int i = 0; i < tab.length; i++)
					tab[i] = null;
				count = 0; // write-volatile
			} finally {
				unlock();
			}
		}
	}

	/* ---------------- Inner Classes -------------- */

	/**
	 * BigConcurrentHashMap list entry. Note that this is never exported
	 * out as a user-visible Map.Entry.
	 *
	 */
	static final class HashEntry {
		volatile long index;
		final int hash;
		HashEntry next;

		HashEntry(long index, int hash, HashEntry next) {
			this.index = index;
			this.hash = hash;
			this.next = next;
		}

		static HashEntry[] newArray(int i) {
			return new HashEntry[i];
		}
	}

}