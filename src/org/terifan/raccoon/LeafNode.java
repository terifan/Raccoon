package org.terifan.raccoon;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import org.terifan.raccoon.io.BlockPointer.BlockType;
import org.terifan.raccoon.util.Log;


/**
 * This is a fixed size buffer for key/value storage suitable for persistence on external media.
 * The LeafNode wraps an array and reads and writes entries directly to the array
 * maintaining all necessary structural information inside the array at all time.
 *
 * implementation notes:
 * - an empty map will always consist of only zero value bytes
 * - the map does not record the capacity, this must be provided when an instance is created
 * - the map have a six byte overhead
 * - each entry have a seven byte overhead
 *
 * Data layout:
 *
 * [header]
 *   2 bytes - entry count
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
class LeafNode implements Iterable<LeafEntry>, Node
{
	private final static int MAX_CAPACITY = 1 << 24;
	private final static int HEADER_SIZE = 2 + 3;
	private final static int ENTRY_POINTER_SIZE = 3;
	private final static int ENTRY_HEADER_SIZE = 2 + 2;
	private final static int MAX_VALUE_SIZE = (1 << 16) - 1;

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
	 * Create a new LeafNode with specified capacity.
	 *
	 * @param aCapacity
	 *   the capacity (length) of the buffer. Maximum 65536 bytes.
	 * @return
	 *   the buffer
	 */
	public LeafNode(int aCapacity)
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

		Stats.leafNodeCreation.incrementAndGet();
	}


	/**
	 * Create a new LeafNode wrapping the provided array.
	 *
	 * @param aBuffer
	 *   the byte array to wrap
	 * @return
	 *   the buffer
	 */
	public LeafNode(byte[] aBuffer)
	{
		this(aBuffer, 0, aBuffer.length);

		Stats.leafNodeCreation.incrementAndGet();
	}


	/**
	 * Create a new LeafNode wrapping the provided array reading the actual map at the specified offset.
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
	public LeafNode(byte[] aBuffer, int aOffset, int aCapacity)
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

		mEntryCount = readInt16(0);
		mFreeSpaceOffset = readInt24(2) + HEADER_SIZE;
		mPointerListOffset = mCapacity - ENTRY_POINTER_SIZE * mEntryCount;

		int limit = (mCapacity - HEADER_SIZE) / (ENTRY_HEADER_SIZE + ENTRY_POINTER_SIZE + 1);

		if (mEntryCount > limit)
		{
			throw new IllegalArgumentException("Entry count exceeds maximum possible entries: " + mEntryCount + ", allowed: " + limit);
		}

		Stats.leafNodeCreation.incrementAndGet();

		assert integrityCheck() == null : integrityCheck();
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.NODE_LEAF;
	}


	public LeafNode clear()
	{
		Arrays.fill(mBuffer, mStartOffset, mStartOffset + mCapacity, (byte)0);

		mEntryCount = 0;
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;

		return this;
	}


	public boolean put(LeafEntry aEntry)
	{
		byte[] key = aEntry.mKey;
		byte[] value = aEntry.mValue;
		byte format = aEntry.mFormat;
		int newValueLengthPlus1 = 1 + value.length;

		if (key.length > MAX_VALUE_SIZE || newValueLengthPlus1 > MAX_VALUE_SIZE || key.length + newValueLengthPlus1 > mCapacity - HEADER_SIZE - ENTRY_HEADER_SIZE - ENTRY_POINTER_SIZE)
		{
			throw new IllegalArgumentException("Entry length exceeds capacity of this map: " + (key.length + newValueLengthPlus1) + " > " + (mCapacity - ENTRY_HEADER_SIZE - HEADER_SIZE - ENTRY_POINTER_SIZE));
		}

		int index = indexOf(key);

		// if key already exists
		if (index >= 0)
		{
			int oldValueLengthPlus1 = readValueLength(index);

			// fast put
			if (oldValueLengthPlus1 == newValueLengthPlus1)
			{
				int entryOffset = readEntryOffset(index);
				int valueOffset = entryOffset + ENTRY_HEADER_SIZE + readKeyLength(index);
				int offset = mStartOffset + valueOffset;

				aEntry.mFormat = mBuffer[offset];
				aEntry.mValue = Arrays.copyOfRange(mBuffer, offset+1, offset + oldValueLengthPlus1);

				System.arraycopy(value, 0, mBuffer, offset+1, value.length);
				mBuffer[offset] = format;

				assert integrityCheck() == null : integrityCheck();

				return true;
			}

			if (newValueLengthPlus1 - oldValueLengthPlus1 > getFreeSpace())
			{
				return false;
			}

			remove(index, aEntry); // old entry value is loaded here

			assert indexOf(key) == (-index) - 1;
		}
		else
		{
			if (getFreeSpace() < ENTRY_HEADER_SIZE + key.length + newValueLengthPlus1 + ENTRY_POINTER_SIZE)
			{
				return false;
			}

			index = (-index) - 1;

			aEntry.mFormat = 0;
			aEntry.mValue = null;
		}

		int modCount = ++mModCount;

		// make room for pointer
		System.arraycopy(mBuffer, mStartOffset + mPointerListOffset, mBuffer, mStartOffset + mPointerListOffset - ENTRY_POINTER_SIZE, ENTRY_POINTER_SIZE * index);
		mPointerListOffset -= ENTRY_POINTER_SIZE;
		mEntryCount++;

		// write entry
		writeEntryOffset(index, mFreeSpaceOffset);
		writeEntryHeader(index, key.length, newValueLengthPlus1);
		int valueOffset = mStartOffset + readValueOffset(index);
		System.arraycopy(key, 0, mBuffer, mStartOffset + readKeyOffset(index), key.length);
		System.arraycopy(value, 0, mBuffer, valueOffset + 1, value.length);
		mBuffer[valueOffset] = format;

		mFreeSpaceOffset += ENTRY_HEADER_SIZE + key.length + newValueLengthPlus1;

		writeBufferHeader();

		assert integrityCheck() == null : integrityCheck();
		assert mModCount == modCount : mModCount + " == " + modCount;

		return true;
	}


	public boolean get(LeafEntry aEntry)
	{
		int index = indexOf(aEntry.mKey);

		if (index < 0)
		{
			return false;
		}

		loadValue(index, aEntry);

		return true;
	}


	private void loadValue(int aIndex, LeafEntry aEntry)
	{
		int valueOffset = mStartOffset + readValueOffset(aIndex);

		aEntry.mFormat = mBuffer[valueOffset];
		aEntry.mValue = Arrays.copyOfRange(mBuffer, valueOffset + 1, valueOffset + readValueLength(aIndex));
	}


	public boolean remove(LeafEntry aEntry)
	{
		int index = indexOf(aEntry.getKey());

		if (index < 0)
		{
			return false;
		}

		remove(index, aEntry);

		return true;
	}


	private void remove(int aIndex, LeafEntry aEntry)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;
		assert aEntry.getKey().length == readKeyLength(aIndex);

		int modCount = ++mModCount;

		int valueOffset = mStartOffset + readValueOffset(aIndex);
		aEntry.mFormat = mBuffer[valueOffset];
		aEntry.mValue = Arrays.copyOfRange(mBuffer, valueOffset+1, valueOffset + readValueLength(aIndex));

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
		writeInt16(0, mEntryCount);
		writeInt24(2, mFreeSpaceOffset - HEADER_SIZE);
	}


	private void writeEntryHeader(int aIndex, int aKeyLength, int aValueLength)
	{
		int i = readEntryOffset(aIndex);

		writeInt16(i + 0, aKeyLength);
		writeInt16(i + 2, aValueLength);
	}


	private int readEntryLength(int aIndex)
	{
		return ENTRY_HEADER_SIZE + readKeyLength(aIndex) + readValueLength(aIndex);
	}


	private void writeEntryOffset(int aIndex, int aOffset)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;
		assert aOffset > 0 && aOffset < mCapacity;

		writeInt24(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE, aOffset);
	}


	private int readEntryOffset(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;

		return readInt24(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE);
	}


	protected int readKeyOffset(int aIndex)
	{
		return readEntryOffset(aIndex) + ENTRY_HEADER_SIZE;
	}


	protected int readKeyLength(int aIndex)
	{
		return readInt16(readEntryOffset(aIndex));
	}


	protected int readValueOffset(int aIndex)
	{
		return readKeyOffset(aIndex) + readKeyLength(aIndex);
	}


	protected int readValueLength(int aIndex)
	{
		return readInt16(readEntryOffset(aIndex) + 2);
	}


	private int readInt16(int aOffset)
	{
		return ((0xff & mBuffer[mStartOffset + aOffset]) << 8) + (0xff & mBuffer[mStartOffset + aOffset + 1]);
	}


	private void writeInt16(int aOffset, int aValue)
	{
		mBuffer[mStartOffset + aOffset + 0] = (byte)(aValue >> 8);
		mBuffer[mStartOffset + aOffset + 1] = (byte)aValue;
	}


	private int readInt24(int aOffset)
	{
		return ((0xff & mBuffer[mStartOffset + aOffset]) << 16) + ((0xff & mBuffer[mStartOffset + aOffset + 1]) << 8) + (0xff & mBuffer[mStartOffset + aOffset + 2]);
	}


	private void writeInt24(int aOffset, int aValue)
	{
		mBuffer[mStartOffset + aOffset + 0] = (byte)(aValue >> 16);
		mBuffer[mStartOffset + aOffset + 1] = (byte)(aValue >> 8);
		mBuffer[mStartOffset + aOffset + 2] = (byte)aValue;
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
				return "Entry offset after free space ("+(offset+" + "+length)+">"+(mStartOffset + mFreeSpaceOffset)+")";
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
		try
		{
			StringBuilder sb = new StringBuilder();
			for (LeafEntry entry : this)
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
			throw new RuntimeException(e);
		}
	}


	@Override
	public Iterator<LeafEntry> iterator()
	{
		return new EntryIterator();
	}


	public class EntryIterator implements Iterator<LeafEntry>
	{
		private final int mExpectedModCount = mModCount;
		private int mIndex;


		@Override
		public boolean hasNext()
		{
			return mIndex < mEntryCount;
		}


		@Override
		public LeafEntry next()
		{
			if (mExpectedModCount != mModCount)
			{
				throw new ConcurrentModificationException();
			}

			int entryOffset = readEntryOffset(mIndex);
			int keyLength = readInt16(entryOffset);
			int offset = mStartOffset + entryOffset + ENTRY_HEADER_SIZE;

			LeafEntry entry = new LeafEntry();
			entry.mKey = Arrays.copyOfRange(mBuffer, offset, offset + keyLength);
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
}