package org.terifan.raccoon.util;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.UUID;
import org.terifan.raccoon.util.Log;


/**
 * This is a fixed size buffer for key/value storage suitable for persistence on external media.
 * The ByteBufferMap wraps an array and reads and writes entries directly to the array
 * maintaining all necessary structural information inside the array at all time.
 *
 * implementation notes:
 * - an empty map will always consist of only ASCII zero bytes
 * - the map does not record the capacity hence this must be provided when an instance is created
 * - the map has a six byte overhead
 * - each entry has a seven byte overhead
 *
 * Data layout:
 *
 * [header]
 *   3 bytes - entry count
 *   3 bytes - free space offset (minus HEADER_SIZE)
 * [list of entries]
 *   (entry 1..n)
 *     2 bytes - key length
 *     2 bytes - value length
 *     n bytes - key
 *     n bytes - value
 * [free space]
 *   n bytes - zeros
 * [list of pointers]
 *   (pointer 1..n)
 *     3 bytes - offset
 */
public class ByteBufferMap implements Iterable<byte[]>
{
	private final static int MAX_CAPACITY = 1 << 24;
	private final static int HEADER_SIZE = 3 + 3;
	private final static int ENTRY_POINTER_SIZE = 3;
	private final static int ENTRY_HEADER_SIZE = 2 + 2;

	private final static int ENTRY_OVERHEAD = ENTRY_POINTER_SIZE + ENTRY_HEADER_SIZE;
	public final static int OVERHEAD = HEADER_SIZE + ENTRY_OVERHEAD + ENTRY_POINTER_SIZE;
	
	public final static byte[] OVERFLOW = UUID.randomUUID().toString().getBytes();

	private byte[] mBuffer;
	private int mStartOffset;
	private int mCapacity;
	private int mPointerListOffset;
	private int mFreeSpaceOffset;
	private int mEntryCount;
	private int mModCount;


	/**
	 * Create a new ByteBufferMap with specified capacity.
	 *
	 * @param aCapacity
	 *   the capacity (length) of the buffer. Maximum 65536 bytes.
	 * @return
	 *   the buffer
	 */
	public ByteBufferMap(int aCapacity)
	{
		if (aCapacity <= HEADER_SIZE || aCapacity > MAX_CAPACITY)
		{
			throw new IllegalArgumentException("Illegal bucket size.");
		}

		mStartOffset = 0;
		mCapacity = aCapacity;
		mBuffer = new byte[aCapacity];
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;
	}


	/**
	 * Create a new ByteBufferMap wrapping the provided array.
	 *
	 * @param aBuffer
	 *   the byte array to wrap
	 * @return
	 *   the buffer
	 */
	public ByteBufferMap(byte[] aBuffer)
	{
		this(aBuffer, 0, aBuffer.length);
	}


	/**
	 * Create a new ByteBufferMap wrapping the provided array reading the actual map at the specified offset.
	 *
	 * @param aBuffer
	 *   the byte array to wrap
	 * @param aOffset
	 *   an offset to the the actual map in the byte array.
	 * @param aCapacity
	 *   the capacity of the buffer, ie. the map use this number of bytes in the byte array provided at the offset specified.
	 * @return
	 *   the buffer
	 */
	public ByteBufferMap(byte[] aBuffer, int aOffset, int aCapacity)
	{
		if (aCapacity > MAX_CAPACITY)
		{
			throw new IllegalArgumentException("Bucket exceeds maximum size.");
		}
		if (aOffset < 0 || aOffset + aCapacity > aBuffer.length)
		{
			throw new IllegalArgumentException("Illegal bucket offset.");
		}

		mBuffer = aBuffer;
		mStartOffset = aOffset;
		mCapacity = aCapacity;
		update();
	}


	public ByteBufferMap update()
	{
		mEntryCount = readTriple(0);
		mFreeSpaceOffset = readTriple(3) + HEADER_SIZE;
		mPointerListOffset = mCapacity - ENTRY_POINTER_SIZE * mEntryCount;

		int limit = (mCapacity - HEADER_SIZE) / (ENTRY_HEADER_SIZE + ENTRY_POINTER_SIZE + 1);

		if (mEntryCount > limit)
		{
			throw new IllegalArgumentException("Entry count exceeds maximum possible entries: " + mEntryCount + ", allowed: " + limit);
		}

		assert integrityCheck() == null : integrityCheck();

		return this;
	}


	public ByteBufferMap clear()
	{
		Arrays.fill(mBuffer, mStartOffset, mStartOffset + mCapacity, (byte)0);

		mEntryCount = 0;
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;

		return this;
	}


	public byte[] put(byte[] aKey, byte[] aValue)
	{
		if (aKey.length > 65535 || aValue.length > 65535 || aKey.length + aValue.length > mCapacity - HEADER_SIZE - ENTRY_HEADER_SIZE - ENTRY_POINTER_SIZE)
		{
			throw new IllegalArgumentException("Entry length exceeds capacity of this map: " + (aKey.length + aValue.length) + " > " + (mCapacity - ENTRY_HEADER_SIZE - HEADER_SIZE - ENTRY_POINTER_SIZE));
		}

		int index = indexOf(aKey);
		byte[] oldValue;

		// if key already exists
		if (index >= 0)
		{
			int oldValueLength = readValueLength(index);

			// fast put
			if (oldValueLength == aValue.length)
			{
				int entryOffset = readEntryOffset(index);
				int valueOffset = entryOffset + ENTRY_HEADER_SIZE + readKeyLength(index);
				oldValue = Arrays.copyOfRange(mBuffer, mStartOffset + valueOffset, mStartOffset + valueOffset + oldValueLength);

				System.arraycopy(aValue, 0, mBuffer, mStartOffset + valueOffset, aValue.length);

				assert integrityCheck() == null : integrityCheck();

				return oldValue;
			}

			if (aValue.length - oldValueLength > getFreeSpace())
			{
				return OVERFLOW;
			}

			oldValue = getValue(index); // todo: skip when copying the map?

			remove(index);

			assert indexOf(aKey) == (-index) - 1;
		}
		else
		{
			if (getFreeSpace() < ENTRY_HEADER_SIZE + aKey.length + aValue.length + ENTRY_POINTER_SIZE)
			{
				return OVERFLOW;
			}

			index = (-index) - 1;

			oldValue = null;
		}

		int modCount = ++mModCount;

		// make room for pointer
		System.arraycopy(mBuffer, mStartOffset + mPointerListOffset, mBuffer, mStartOffset + mPointerListOffset - ENTRY_POINTER_SIZE, ENTRY_POINTER_SIZE * index);
		mPointerListOffset -= ENTRY_POINTER_SIZE;
		mEntryCount++;

		// write entry
		writeEntryOffset(index, mFreeSpaceOffset);
		writeEntryHeader(index, aKey.length, aValue.length);
		System.arraycopy(aKey, 0, mBuffer, mStartOffset + readKeyOffset(index), aKey.length);
		System.arraycopy(aValue, 0, mBuffer, mStartOffset + readValueOffset(index), aValue.length);

		mFreeSpaceOffset += ENTRY_HEADER_SIZE + aKey.length + aValue.length;

		writeBufferHeader();

		assert integrityCheck() == null : integrityCheck();
		assert mModCount == modCount : mModCount + " == " + modCount;

		return oldValue;
	}


	public byte[] get(byte[] aKey)
	{
		return getValue(indexOf(aKey));
	}


	public byte[] getValue(int aIndex)
	{
		if (aIndex < 0)
		{
			return null;
		}

		int modCount = mModCount;

		int offset = mStartOffset + readValueOffset(aIndex);
		byte[] value = Arrays.copyOfRange(mBuffer, offset, offset + readValueLength(aIndex));

		assert mModCount == modCount : mModCount + " == " + modCount;

		return value;
	}


	public byte[] getKey(int aIndex)
	{
		int modCount = mModCount;

		int entryOffset = readEntryOffset(aIndex);
		int keyLength = readShort(entryOffset);
		int offset = mStartOffset + entryOffset + ENTRY_HEADER_SIZE;

		byte[] key = Arrays.copyOfRange(mBuffer, offset, offset + keyLength);

		assert mModCount == modCount : mModCount + " == " + modCount;

		return key;
	}


	public byte[] floorKey(byte[] aKey)
	{
		int index = indexOf(aKey);

		if (index >= 0)
		{
			return aKey;
		}

		index = (-index) - 1 - 1;

		if (index < 0)
		{
			return null;
		}

		return getKey(index);
	}


	public byte[] ceilingKey(byte[] aKey)
	{
		int index = indexOf(aKey);

		if (index >= 0)
		{
			return aKey;
		}

		index = (-index) - 1;

		if (index == mEntryCount)
		{
			return null;
		}

		return getKey(index);
	}


	public byte[] firstKey()
	{
		return getKey(0);
	}


	public byte[] lastKey()
	{
		return getKey(mEntryCount - 1);
	}


	public byte[] remove(byte[] aKey)
	{
		int index = indexOf(aKey);

		if (index < 0)
		{
			return null;
		}

		int modCount = mModCount;

		int offset = mStartOffset + readValueOffset(index);
		byte[] value = Arrays.copyOfRange(mBuffer, offset, offset + readValueLength(index));

		assert mModCount == modCount : mModCount + " == " + modCount;

		remove(index);

		return value;
	}


	public boolean containsKey(byte[] aKey)
	{
		return indexOf(aKey) >= 0;
	}


	private void remove(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;

		int modCount = ++mModCount;

		int offset = readEntryOffset(aIndex);
		int length = readEntryLength(aIndex);

		assert mStartOffset + offset + length <= mStartOffset + mFreeSpaceOffset;

		// compact the record list
		System.arraycopy(mBuffer, mStartOffset + offset + length, mBuffer, mStartOffset + offset, mFreeSpaceOffset - offset - length);

		// compact the pointer list
		System.arraycopy(mBuffer, mStartOffset + mPointerListOffset, mBuffer, mStartOffset + mPointerListOffset + ENTRY_POINTER_SIZE, ENTRY_POINTER_SIZE * aIndex);

		// remove pointer
		fill(mBuffer, mStartOffset + mPointerListOffset, ENTRY_POINTER_SIZE);

		mEntryCount--;
		mFreeSpaceOffset -= length;
		mPointerListOffset += ENTRY_POINTER_SIZE;

		// clear the unused area
		fill(mBuffer, mStartOffset + mFreeSpaceOffset, length);

		// adjust pointers
		for (int i = 0; i < mEntryCount; i++)
		{
			int o = readEntryOffset(i);
			if (o >= offset)
			{
				writeEntryOffset(i, o - length);
			}
		}

		writeBufferHeader();

		assert integrityCheck() == null : integrityCheck();
		assert mModCount == modCount : mModCount + " == " + modCount;
	}


	public byte[] array()
	{
		return mBuffer;
	}


	public int size()
	{
		return mEntryCount;
	}


	public int capacity()
	{
		return mCapacity;
	}


	public boolean isEmpty()
	{
		return mEntryCount == 0;
	}


	public int getFreeSpace()
	{
		assert mPointerListOffset - mFreeSpaceOffset >= 0 : mPointerListOffset + " " + mFreeSpaceOffset;

		return mPointerListOffset - mFreeSpaceOffset;
	}


	public int getStartOffset()
	{
		return mStartOffset;
	}


	/**
	 * Optimized method for copying an entry from this map to another map. Equivalent of calling:
	 *
	 * <pre>
	 *	to.put(key, from.getValue(key));
	 * </pre>
	 */
	public void copy(int aIndex, ByteBufferMap aDestinationMap)
	{
		int modCount = mModCount;

		int entryOffset = readEntryOffset(aIndex);
		int keyLength = readShort(entryOffset);
		int valueLength = readShort(entryOffset + 2);
		int offset = mStartOffset + entryOffset + ENTRY_HEADER_SIZE;

		byte[] key = Arrays.copyOfRange(mBuffer, offset, offset + keyLength);
		byte[] value = Arrays.copyOfRange(mBuffer, offset + keyLength, offset + keyLength + valueLength);

		assert mModCount == modCount : mModCount + " == " + modCount;

		byte[] oldValue = aDestinationMap.put(key, value);
		
		assert oldValue != OVERFLOW;
	}


	private int indexOf(byte[] aKey)
	{
		int low = 0;
		int high = mEntryCount - 1;

		while (low <= high)
		{
			int mid = (low + high) >>> 1;

			int cmp = compare(aKey, 0, aKey.length, mBuffer, mStartOffset + readKeyOffset(mid), readKeyLength(mid));

			if (cmp > 0)
			{
				low = mid + 1;
			}
			else if (cmp < 0)
			{
				high = mid - 1;
			}
			else
			{
				return mid; // key found
			}
		}

		return -(low + 1); // key not found
	}


	private int compare(byte[] aBufferA, int aOffsetA, int aLengthA, byte[] aBufferB, int aOffsetB, int aLengthB)
	{
		for (int end = aOffsetA + Math.min(aLengthA, aLengthB); aOffsetA < end; aOffsetA++, aOffsetB++)
		{
			byte a = aBufferA[aOffsetA];
			byte b = aBufferB[aOffsetB];
			if (a != b)
			{
				return (255 & a) - (255 & b);
			}
		}

		return aLengthA - aLengthB;
	}


	private static void fill(byte[] aBuffer, int aOffset, int aLength)
	{
		for (int i = aOffset, j = aOffset + aLength; i < j; i++)
		{
			aBuffer[i] = 0;
		}
	}


	private void writeBufferHeader()
	{
		writeTriple(0, mEntryCount);
		writeTriple(3, mFreeSpaceOffset - HEADER_SIZE);
	}


	private void writeEntryHeader(int aIndex, int aKeyLength, int aValueLength)
	{
		int i = readEntryOffset(aIndex);

		writeShort(i + 0, aKeyLength);
		writeShort(i + 2, aValueLength);
	}


	private int readEntryLength(int aIndex)
	{
		return ENTRY_HEADER_SIZE + readKeyLength(aIndex) + readValueLength(aIndex);
	}


	private void writeEntryOffset(int aIndex, int aOffset)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;
		assert aOffset > 0 && aOffset < mCapacity;

		writeTriple(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE, aOffset);
	}


	private int readEntryOffset(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;

		return readTriple(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE);
	}


	protected int readKeyOffset(int aIndex)
	{
		return readEntryOffset(aIndex) + ENTRY_HEADER_SIZE;
	}


	protected int readKeyLength(int aIndex)
	{
		return readShort(readEntryOffset(aIndex));
	}


	protected int readValueOffset(int aIndex)
	{
		return readKeyOffset(aIndex) + readKeyLength(aIndex);
	}


	protected int readValueLength(int aIndex)
	{
		return readShort(readEntryOffset(aIndex) + 2);
	}


	private int readShort(int aOffset)
	{
		return ((0xff & mBuffer[mStartOffset + aOffset]) << 8) + (0xff & mBuffer[mStartOffset + aOffset + 1]);
	}


	private void writeShort(int aOffset, int aValue)
	{
		mBuffer[mStartOffset + aOffset + 0] = (byte)(aValue >> 8);
		mBuffer[mStartOffset + aOffset + 1] = (byte)aValue;
	}


	private int readTriple(int aOffset)
	{
		return ((0xff & mBuffer[mStartOffset + aOffset]) << 16) + ((0xff & mBuffer[mStartOffset + aOffset + 1]) << 8) + (0xff & mBuffer[mStartOffset + aOffset + 2]);
	}


	private void writeTriple(int aOffset, int aValue)
	{
		mBuffer[mStartOffset + aOffset + 0] = (byte)(aValue >> 16);
		mBuffer[mStartOffset + aOffset + 1] = (byte)(aValue >> 8);
		mBuffer[mStartOffset + aOffset + 2] = (byte)aValue;
	}


	@Override
	public Iterator<byte[]> iterator()
	{
		return new Iterator<byte[]>()
		{
			private int mExpectedModCount = mModCount;
			private int mIndex;


			@Override
			public boolean hasNext()
			{
				if (mExpectedModCount != mModCount)
				{
					throw new ConcurrentModificationException();
				}

				return mIndex < mEntryCount;
			}


			@Override
			public byte[] next()
			{
				if (mExpectedModCount != mModCount)
				{
					throw new ConcurrentModificationException();
				}

				return getKey(mIndex++);
			}


			@Override
			public void remove()
			{
				if (mExpectedModCount != mModCount)
				{
					throw new ConcurrentModificationException();
				}
				if (mIndex == 0 || mIndex > mEntryCount)
				{
					throw new IllegalStateException();
				}

				ByteBufferMap.this.remove(mIndex - 1);
				mExpectedModCount = mModCount;
			}
		};
	}


	public String integrityCheck()
	{
		int free = mPointerListOffset - mFreeSpaceOffset;

		if (mPointerListOffset != mCapacity - ENTRY_POINTER_SIZE * mEntryCount)
		{
			return "Pointer list has bad offset";
		}

		if (mFreeSpaceOffset + free != mPointerListOffset)
		{
			return "Free space doesn't end at point list";
		}

		for (int i = 0; i < free; i++)
		{
			if (mBuffer[mStartOffset + mFreeSpaceOffset + i] != 0)
			{
				return "Free space contains data";
			}
		}

		int dataLength = 0;

		for (int i = 0, prevKeyOffset = -1, prevKeyLength = -1; i < mEntryCount; i++)
		{
			int offset = mStartOffset + readEntryOffset(i);
			int keyOffset = mStartOffset + readKeyOffset(i);
			int keyLength = readKeyLength(i);
			int length = readEntryLength(i);

			dataLength += length;

			if (offset < mStartOffset + HEADER_SIZE)
			{
				return "Entry offset before header";
			}
			if (offset + length > mStartOffset + mFreeSpaceOffset)
			{
				return "Entry offset after free space";
			}

			if (i > 0 && compare(mBuffer, prevKeyOffset, prevKeyLength, mBuffer, keyOffset, keyLength) >= 0)
			{
				return "Keys are out of order";
			}

			prevKeyOffset = keyOffset;
			prevKeyLength = keyLength;
		}

		if (HEADER_SIZE + dataLength + free + ENTRY_POINTER_SIZE * mEntryCount != mCapacity)
		{
			return "Capacity don't match match actual size of map";
		}

		return null;
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (byte[] k : this)
		{
			if (sb.length() > 0)
			{
				sb.append(", ");
			}
			sb.append(new String(k));
		}
		return "[" + sb.toString() + "]";
	}
}
