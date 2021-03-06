package org.terifan.raccoon;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Result;


/**
 * This is a fixed size buffer for key/value storage suitable for persistence on external media. An array is wrapped and read and
 * written to directly maintaining all necessary structural information inside the array at all time.
 *
 * implementation note: an empty map will always consist of only zero value bytes, the map does not record the capacity, this must be
 * provided when an instance is created
 *
 * Data layout:
 *
 * [header]
 *   2 bytes - entry count
 *   4 bytes - free space offset (minus HEADER_SIZE)
 * [list of entries]
 *   (entry 1..n)
 *   2 bytes - key length
 *   2 bytes - value length
 *   n bytes - key
 *   n bytes - value
 * [free space]
 *   n bytes - zeros
 * [list of pointers]
 *   (pointer 1..n)
 *   4 bytes - offset
 */
public class ArrayMap implements Iterable<ArrayMapEntry>
{
	private final static int HEADER_SIZE = 2 + 4;
	private final static int ENTRY_POINTER_SIZE = 4;
	private final static int ENTRY_HEADER_SIZE = 2 + 2;

	public final static int MAX_VALUE_SIZE = (1 << 16) - 1;
	public final static int MAX_ENTRY_COUNT = (1 << 16) - 1;

	public final static int EXACT = 0;
	public final static int NEAR = 1;
	public final static int LAST = 2;

	private final static int ENTRY_OVERHEAD = ENTRY_POINTER_SIZE + ENTRY_HEADER_SIZE;
	public final static int OVERHEAD = HEADER_SIZE + ENTRY_OVERHEAD + ENTRY_POINTER_SIZE;

	private byte[] mBuffer;
	private int mStartOffset;
	private int mCapacity;
	private int mPointerListOffset;
	private int mFreeSpaceOffset;
	private int mEntryCount;
	private int mModCount;


	/**
	 * Create a new ArrayMap with specified capacity.
	 *
	 * @param aCapacity the capacity (length) of the buffer. Maximum 65536 bytes.
	 * @return the buffer
	 */
	public ArrayMap(int aCapacity)
	{
		if (aCapacity <= HEADER_SIZE)
		{
			throw new IllegalArgumentException("Illegal bucket size: " + aCapacity);
		}

		mStartOffset = 0;
		mCapacity = aCapacity;
		mBuffer = new byte[aCapacity];
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;
	}


	/**
	 * Create a new ArrayMap wrapping the provided array.
	 *
	 * @param aBuffer the byte array to wrap
	 * @return the buffer
	 */
	public ArrayMap(byte[] aBuffer)
	{
		this(aBuffer, 0, aBuffer.length);
	}


	/**
	 * Create a new ArrayMap wrapping the provided array reading the actual map at the specified offset.
	 *
	 * @param aBuffer the byte array to wrap
	 * @param aOffset an offset to the the actual map in the byte array.
	 * @param aCapacity the capacity of the buffer, ie. the map use this number of bytes in the byte array provided at the offset specified.
	 * @return the buffer
	 */
	public ArrayMap(byte[] aBuffer, int aOffset, int aCapacity)
	{
		if (aOffset < 0 || aOffset + aCapacity > aBuffer.length)
		{
			throw new IllegalArgumentException("Illegal bucket offset.");
		}

		mBuffer = aBuffer;
		mStartOffset = aOffset;
		mCapacity = aCapacity;

		mEntryCount = readInt16(0);
		mFreeSpaceOffset = readInt32(2) + HEADER_SIZE;
		mPointerListOffset = mCapacity - ENTRY_POINTER_SIZE * mEntryCount;

		int limit = (mCapacity - HEADER_SIZE) / (ENTRY_HEADER_SIZE + ENTRY_POINTER_SIZE);

		if (mEntryCount > limit)
		{
			throw new IllegalArgumentException("Entry count exceeds maximum possible entries: " + mEntryCount + ", allowed: " + limit);
		}

		assert integrityCheck() == null : integrityCheck();
	}


	public int getCapacity()
	{
		return mCapacity;
	}


	public byte[] array()
	{
		return mBuffer;
	}


	public ArrayMap clear()
	{
		Arrays.fill(mBuffer, mStartOffset, mStartOffset + mCapacity, (byte)0);

		mEntryCount = 0;
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;

		return this;
	}


	public boolean put(ArrayMapEntry aEntry, Result<ArrayMapEntry> oExistingEntry)
	{
		byte[] key = aEntry.getKey();
		int valueLength = aEntry.getMarshalledValueLength();
		int keyLength = key.length;

		if (keyLength > MAX_VALUE_SIZE || valueLength > MAX_VALUE_SIZE || keyLength + valueLength > mCapacity - HEADER_SIZE - ENTRY_HEADER_SIZE - ENTRY_POINTER_SIZE)
		{
			throw new IllegalArgumentException("Entry length exceeds capacity of this map: " + (keyLength + valueLength) + " > " + (mCapacity - ENTRY_HEADER_SIZE - HEADER_SIZE - ENTRY_POINTER_SIZE));
		}

		int index = indexOf(key);

		// if key already exists
		if (index >= 0)
		{
			int entryOffset = readEntryOffset(index);
			int oldValueLength = readValueLength(entryOffset);

			// replace with same length
			if (oldValueLength == valueLength)
			{
				int valueOffset = readValueOffset(entryOffset);

				if (oExistingEntry != null)
				{
					ArrayMapEntry old = new ArrayMapEntry();
					old.setKey(key);
					old.unmarshallValue(mBuffer, mStartOffset + valueOffset, oldValueLength);
					oExistingEntry.set(old);
				}

				aEntry.marshallValue(mBuffer, mStartOffset + valueOffset);

				assert integrityCheck() == null : integrityCheck();

				return true;
			}

			if (valueLength - oldValueLength > getFreeSpace())
			{
				return false;
			}

			removeImpl(index, oExistingEntry);

			assert indexOf(key) == (-index) - 1;
		}
		else if (getFreeSpace() < ENTRY_HEADER_SIZE + keyLength + valueLength + ENTRY_POINTER_SIZE)
		{
			return false;
		}
		else
		{
			index = (-index) - 1;
		}

		if (++mEntryCount > MAX_ENTRY_COUNT)
		{
			return false;
		}

		int modCount = ++mModCount;

		// make room for pointer
		System.arraycopy(mBuffer, mStartOffset + mPointerListOffset, mBuffer, mStartOffset + mPointerListOffset - ENTRY_POINTER_SIZE, ENTRY_POINTER_SIZE * index);
		mPointerListOffset -= ENTRY_POINTER_SIZE;

		// write entry
		int entryOffset = mFreeSpaceOffset;
		writeEntryOffset(index, entryOffset);
		writeKeyLength(entryOffset, keyLength);
		writeValueLength(entryOffset, valueLength);
		System.arraycopy(key, 0, mBuffer, mStartOffset + readKeyOffset(entryOffset), keyLength);
		aEntry.marshallValue(mBuffer, mStartOffset + readValueOffset(entryOffset));

		mFreeSpaceOffset += ENTRY_HEADER_SIZE + keyLength + valueLength;

		writeBufferHeader();

		assert integrityCheck() == null : integrityCheck();
		assert mModCount == modCount : mModCount + " == " + modCount;

		return true;
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		int index = indexOf(aEntry.getKey());

		if (index < 0)
		{
			return false;
		}

		loadValue(index, aEntry);

		return true;
	}


	/**
	 * Find an entry equal or before the sought key
	 *
	 * @return
	 *   one of NEAR, EXACT or LAST depending on what entry was found. LAST indicated no identical or smaller key was found.
	 */
	public int nearest(ArrayMapEntry aEntry)
	{
		int index = indexOf(aEntry.getKey());

		if (index == -mEntryCount - 1)
		{
			return LAST;
		}
		if (index < 0)
		{
			loadValue(-index - 1, aEntry);

			return NEAR;
		}

		loadValue(index, aEntry);

		return EXACT;
	}


	private void loadValue(int aIndex, ArrayMapEntry aEntry)
	{
		int entryOffset = readEntryOffset(aIndex);
		int valueOffset = readValueOffset(entryOffset);
		int valueLength = readValueLength(entryOffset);

		aEntry.unmarshallValue(mBuffer, mStartOffset + valueOffset, valueLength);
	}


	public boolean remove(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry)
	{
		int index = indexOf(aEntry.getKey());

		if (index < 0)
		{
			return false;
		}

		removeImpl(index, oOldEntry);

		return true;
	}


	private void removeImpl(int aIndex, Result<ArrayMapEntry> oOldEntry)
	{
		assert aIndex >= 0 && aIndex < mEntryCount : "index="+aIndex+", count="+mEntryCount;

		int modCount = ++mModCount;

		if (oOldEntry != null)
		{
			ArrayMapEntry old = new ArrayMapEntry();
			get(aIndex, old);
			oOldEntry.set(old);
		}

		int offset = readEntryOffset(aIndex);
		int length = readEntryLength(offset);

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


	public ArrayMapEntry get(int aIndex, ArrayMapEntry aEntry)
	{
		int entryOffset = readEntryOffset(aIndex);
		int keyOffset = readKeyOffset(entryOffset);
		int keyLength = readKeyLength(entryOffset);
		int valueOffset = readValueOffset(entryOffset);
		int valueLength = readValueLength(entryOffset);

		aEntry.unmarshallKey(mBuffer, mStartOffset + keyOffset, keyLength);
		aEntry.unmarshallValue(mBuffer, mStartOffset + valueOffset, valueLength);

		return aEntry;
	}


	public int size()
	{
		return mEntryCount;
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


	public int indexOf(byte[] aKey)
	{
		int low = 0;
		int high = mEntryCount - 1;

		while (low <= high)
		{
			int mid = (low + high) >>> 1;

			int entryOffset = readEntryOffset(mid);
			int keyOffset = readKeyOffset(entryOffset);
			int keyLength = readKeyLength(entryOffset);

			int cmp = compare(aKey, 0, aKey.length, mBuffer, mStartOffset + keyOffset, keyLength);

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
		writeInt16(0, mEntryCount);
		writeInt32(2, mFreeSpaceOffset - HEADER_SIZE);
	}


	private int readEntryLength(int aEntryOffset)
	{
		return ENTRY_HEADER_SIZE + readKeyLength(aEntryOffset) + readValueLength(aEntryOffset);
	}


	private int readEntryOffset(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;

		return readInt32(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE);
	}


	private void writeEntryOffset(int aIndex, int aEntryOffset)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;
		assert aEntryOffset > 0 && aEntryOffset < mCapacity;

		writeInt32(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE, aEntryOffset);
	}


	private int readKeyOffset(int aEntryOffset)
	{
		return aEntryOffset + ENTRY_HEADER_SIZE;
	}


	private int readKeyLength(int aEntryOffset)
	{
		return readInt16(aEntryOffset);
	}


	private void writeKeyLength(int aEntryOffset, int aKeyLength)
	{
		writeInt16(aEntryOffset, aKeyLength);
	}


	private int readValueOffset(int aEntryOffset)
	{
		return aEntryOffset + ENTRY_HEADER_SIZE + readKeyLength(aEntryOffset);
	}


	private int readValueLength(int aEntryOffset)
	{
		return readInt16(aEntryOffset + 2);
	}


	private void writeValueLength(int aEntryOffset, int aValueLength)
	{
		writeInt16(aEntryOffset + 2, aValueLength);
	}


	private int readInt16(int aOffset)
	{
		return ByteArrayUtil.getInt16(mBuffer, mStartOffset + aOffset);
	}


	private void writeInt16(int aOffset, int aValue)
	{
		ByteArrayUtil.putInt16(mBuffer, mStartOffset + aOffset, aValue);
	}


	private int readInt32(int aOffset)
	{
		return ByteArrayUtil.getInt32(mBuffer, mStartOffset + aOffset);
	}


	private void writeInt32(int aOffset, int aValue)
	{
		ByteArrayUtil.putInt32(mBuffer, mStartOffset + aOffset, aValue);
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
			int entryOffset = readEntryOffset(i);
			int bufferOffset = entryOffset;
			int keyOffset = readKeyOffset(entryOffset);
			int keyLength = readKeyLength(entryOffset);
			int length = readEntryLength(entryOffset);

			dataLength += length;

			if (bufferOffset < HEADER_SIZE)
			{
				return "Entry offset before header";
			}
			if (bufferOffset + length > mFreeSpaceOffset)
			{
				return "Entry offset after free space (" + ((mStartOffset + bufferOffset) + " + " + length) + ">" + (mStartOffset + mFreeSpaceOffset) + ")";
			}
			if (i > 0 && compare(mBuffer, mStartOffset + prevKeyOffset, prevKeyLength, mBuffer, mStartOffset + keyOffset, keyLength) >= 0)
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
		try
		{
			StringBuilder sb = new StringBuilder();
			for (ArrayMapEntry entry : this)
			{
				if (sb.length() > 0)
				{
					sb.append(", ");
				}
				sb.append(new String(entry.getKey(), "utf-8"));
			}
			return "[" + sb.toString() + "]";
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalStateException(e);
		}
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		return new EntryIterator();
	}


	public class EntryIterator implements Iterator<ArrayMapEntry>
	{
		private final int mExpectedModCount = mModCount;
		private int mIndex;


		@Override
		public boolean hasNext()
		{
			return mIndex < mEntryCount;
		}


		@Override
		public ArrayMapEntry next()
		{
			if (mExpectedModCount != mModCount)
			{
				throw new ConcurrentModificationException();
			}
			if (mIndex >= mEntryCount)
			{
				throw new NoSuchElementException();
			}

			int entryOffset = readEntryOffset(mIndex);
			int keyLength = readKeyLength(entryOffset);
			int keyOffset = readKeyOffset(entryOffset);

			ArrayMapEntry entry = new ArrayMapEntry();
			entry.unmarshallKey(mBuffer, mStartOffset + keyOffset, keyLength);
			loadValue(mIndex, entry);

			mIndex++;

			return entry;
		}


		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}


	public ArrayMapEntry getFirst()
	{
		ArrayMapEntry entry = new ArrayMapEntry();
		get(0, entry);
		return entry;
	}


	public ArrayMapEntry getLast()
	{
		ArrayMapEntry entry = new ArrayMapEntry();
		get(mEntryCount - 1, entry);
		return entry;
	}


	public ArrayMapEntry removeFirst()
	{
		Result<ArrayMapEntry> entry = new Result<>();
		removeImpl(0, entry);
		return entry.get();
	}


	public ArrayMapEntry removeLast()
	{
		Result<ArrayMapEntry> entry = new Result<>();
		removeImpl(mEntryCount - 1, entry);
		return entry.get();
	}


	public ArrayMap resize(int aNewSize)
	{
		int s = ENTRY_POINTER_SIZE * mEntryCount;
		byte[] buffer = new byte[aNewSize];

		System.arraycopy(mBuffer, 0, buffer, 0, mFreeSpaceOffset);
		System.arraycopy(mBuffer, mBuffer.length - s, buffer, aNewSize - s, s);

		return new ArrayMap(buffer);
	}
}
