package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt16;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.getInt32;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt16;
import static org.terifan.raccoon.blockdevice.util.ByteArrayUtil.putInt32;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;


/**
 * This is a fixed size buffer for key/value storage suitable for persistence on external media. An array is wrapped and read and written to
 * directly maintaining all necessary structural information inside the array at all time.
 * <p>
 * Implementation note: an empty map will always consist of only zero value bytes, the map does not record the capacity, this must be
 * provided when an instance is created
 * </p>
 * <pre>
 * Data layout:
 *
 * [header]
 *    2 bytes - entry count
 *    4 bytes - free space offset (minus HEADER_SIZE)
 * [list of entries]
 *    (entry 1..n)
 *    2 bytes - key length
 *    2 bytes - value length
 *    1 bytes - key header
 *    n bytes - key
 *    1 bytes - value header
 *    n bytes - value
 * [free space]
 *    n bytes - zeros
 * [list of pointers]
 *    (pointer 1..n)
 *    4 bytes - offset
 * </pre>
 */
class ArrayMap implements Iterable<ArrayMapEntry>
{
	final static int HEADER_SIZE = 2 + 4;
	final static int ENTRY_POINTER_SIZE = 4;
	final static int ENTRY_HEADER_SIZE = 2 + 2;

	public final static int MAX_VALUE_SIZE = (1 << 16) - 1;
	public final static int MAX_ENTRY_COUNT = (1 << 16) - 1;

	private final static int ENTRY_OVERHEAD = ENTRY_POINTER_SIZE + ENTRY_HEADER_SIZE;
	public final static int OVERHEAD = HEADER_SIZE + ENTRY_OVERHEAD + ENTRY_POINTER_SIZE;

	private byte[] mBuffer;
	private final int mStartOffset = 0;
	private int mCapacity;
	private int mCapacityGrowth;
	private int mPointerListOffset;
	private int mFreeSpaceOffset;
	private int mEntryCount;
	private int mModCount;


	public ArrayMap(int aCapacity, int aCapacityGrowth)
	{
		if (aCapacity <= HEADER_SIZE)
		{
			throw new IllegalArgumentException("Illegal bucket size: " + aCapacity);
		}

//		mStartOffset = 0;
		mCapacity = aCapacity;
		mCapacityGrowth = aCapacityGrowth;
		mBuffer = new byte[aCapacity];
		mFreeSpaceOffset = HEADER_SIZE;
		mPointerListOffset = mCapacity;
	}


	public ArrayMap(byte[] aBuffer, int aCapacityGrowth)
	{
//		this(aBuffer, 0, aBuffer.length);
//	}
//
//
//	public ArrayMap(byte[] aBuffer, int aOffset, int aCapacity)
//	{
//		if (aOffset < 0 || aOffset + aCapacity > aBuffer.length)
//		{
//			throw new IllegalArgumentException("Illegal bucket offset.");
//		}

		mBuffer = aBuffer;
//		mStartOffset = 0;
		mCapacity = aBuffer.length;
		mCapacityGrowth = aCapacityGrowth;
//		mStartOffset = aOffset;
//		mCapacity = aCapacity;

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


	public ArrayMap setCapacityGrowth(int aCapacityGrowth)
	{
		mCapacityGrowth = aCapacityGrowth;
		return this;
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


	public void insert(ArrayMapEntry aEntry)
	{
		assert mCapacityGrowth > 0;

		put(aEntry);

		if (aEntry.getState() != OpState.OVERFLOW)
		{
			return;
		}

		resize((mCapacity + ENTRY_OVERHEAD + aEntry.getMarshalledLength() + mCapacityGrowth - 1) / mCapacityGrowth * mCapacityGrowth);

		put(aEntry);

		if (aEntry.getState() == OpState.OVERFLOW)
		{
			throw new IllegalStateException("failed to put entity");
		}
	}


	public void put(ArrayMapEntry aEntry)
	{
		int keyLength = aEntry.getMarshalledKeyLength();
		int valueLength = aEntry.getMarshalledValueLength();

		byte[] oldValue = null;

		if (keyLength > MAX_VALUE_SIZE || valueLength > MAX_VALUE_SIZE || keyLength + valueLength > mCapacity - HEADER_SIZE - ENTRY_HEADER_SIZE - ENTRY_POINTER_SIZE)
		{
			aEntry.setState(OpState.OVERFLOW);
			return;
		}

		int index = indexOf(aEntry);

		// if key already exists
		if (index >= 0)
		{
			aEntry.setState(OpState.UPDATE);

			int entryOffset = readEntryOffset(index);
			int oldValueLength = readValueLength(entryOffset);
			int valueOffset = readValueOffset(entryOffset);

			// replace with same length
			if (oldValueLength == valueLength)
			{
				byte[] buffer = aEntry.getValue();
				for (int i = 0, j = mStartOffset + valueOffset + 1; i < oldValueLength - 1; i++, j++)
				{
					byte b = buffer[i];
					buffer[i] = mBuffer[j];
					mBuffer[j] = b;
				}

				assert integrityCheck() == null : integrityCheck();

				return;
			}

			if (valueLength - oldValueLength > getFreeSpace())
			{
				aEntry.setState(OpState.OVERFLOW);
				return;
			}

			oldValue = Arrays.copyOfRange(mBuffer, mStartOffset + valueOffset, mStartOffset + valueOffset + oldValueLength);

			removeImpl(index);

			assert indexOf(aEntry) == (-index) - 1;

			// continue permform put
		}
		else if (getFreeSpace() < ENTRY_HEADER_SIZE + keyLength + valueLength + ENTRY_POINTER_SIZE)
		{
			aEntry.setState(OpState.OVERFLOW);
			return;
		}
		else
		{
			aEntry.setState(OpState.INSERT);
			index = (-index) - 1;
		}

		if (++mEntryCount > MAX_ENTRY_COUNT)
		{
			aEntry.setState(OpState.OVERFLOW);
			return;
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

		assert keyLength == aEntry.getMarshalledKeyLength();
		assert valueLength == aEntry.getMarshalledValueLength();

		aEntry.getMarshalledKey(mBuffer, mStartOffset + readKeyOffset(entryOffset));
		aEntry.getMarshalledValue(mBuffer, mStartOffset + readValueOffset(entryOffset));

		mFreeSpaceOffset += ENTRY_HEADER_SIZE + keyLength + valueLength;

		writeBufferHeader();

		assert integrityCheck() == null : integrityCheck();
		assert mModCount == modCount : mModCount + " == " + modCount;

		if (oldValue != null)
		{
			aEntry.setMarshalledValue(oldValue, 0, oldValue.length);
		}
	}


	public ArrayMapEntry get(ArrayMapEntry aEntry)
	{
		int index = indexOf(aEntry);

		if (index < 0)
		{
			ArrayMapEntry result = new ArrayMapEntry();
			result.setState(OpState.NO_MATCH);
			return result;
		}

		loadKeyAndValue(index, aEntry);

		aEntry.setState(OpState.MATCH);
		return aEntry;
	}


	public int nearestIndex(ArrayMapEntry aKey)
	{
		int index = indexOf(aKey);

		if (index == -mEntryCount - 1)
		{
			index = mEntryCount - 1;
		}
		else if (index < 0)
		{
			index = Math.max(0, -index - 2);
		}

		return index;
	}


	public int loadNearestEntry(ArrayMapEntry aEntry)
	{
		int index = nearestIndex(aEntry);
		loadKeyAndValue(index, aEntry);
		return index;
	}


	/**
	 * Find the index of the actual key or the index of a key larger than the one provided.
	 */
	public int findEntry(ArrayMapEntry aKey)
	{
		int index = indexOf(aKey);

		if (index < 0)
		{
			index = -index - 1;
		}

		return index;
	}


	public int findEntryAfter(ArrayMapEntry aKey)
	{
		int index = indexOf(aKey);

		if (index < 0)
		{
			index = -index - 1;
		}
		else
		{
			index++;
		}

		return index;
	}


	ArrayMapEntry loadKeyAndValue(int aIndex, ArrayMapEntry aEntry)
	{
		int entryOffset = readEntryOffset(aIndex);
		int valueOffset = readValueOffset(entryOffset);
		int valueLength = readValueLength(entryOffset);
		int keyOffset = readKeyOffset(entryOffset);
		int keyLength = readKeyLength(entryOffset);

		aEntry.setMarshalledKey(mBuffer, mStartOffset + keyOffset, keyLength);
		aEntry.setMarshalledValue(mBuffer, mStartOffset + valueOffset, valueLength);
		return aEntry;
	}


	public void remove(int aIndex)
	{
		removeImpl(aIndex);
	}


	public void remove(ArrayMapEntry aKey)
	{
		int index = indexOf(aKey);

		if (index < 0)
		{
			aKey.setState(OpState.NO_MATCH);
		}
		else
		{
			aKey.setState(OpState.REMOVED);
			removeImpl(index);
		}
	}


	private void removeImpl(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount : "index=" + aIndex + ", count=" + mEntryCount;

		int modCount = ++mModCount;

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


	public ArrayMapEntry get(int aIndex)
	{
		assert aIndex < size() : "out of bounds: index: " + aIndex + ", size: " + size();

		int entryOffset = readEntryOffset(aIndex);
		int keyOffset = readKeyOffset(entryOffset);
		int keyLength = readKeyLength(entryOffset);
		int valueOffset = readValueOffset(entryOffset);
		int valueLength = readValueLength(entryOffset);

		return new ArrayMapEntry()
			.setMarshalledKey(mBuffer, mStartOffset + keyOffset, keyLength)
			.setMarshalledValue(mBuffer, mStartOffset + valueOffset, valueLength);
	}


	public ArrayMapEntry getKey(int aIndex)
	{
		assert aIndex < size() : "index out of bounds: " + aIndex + ", size: " + size();

		int entryOffset = readEntryOffset(aIndex);
		int keyOffset = readKeyOffset(entryOffset);
		int keyLength = readKeyLength(entryOffset);

		return new ArrayMapEntry().setMarshalledKey(mBuffer, mStartOffset + keyOffset, keyLength);
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


	public int getUsedSpace()
	{
		return mCapacity - getFreeSpace();
	}


	public int indexOf(ArrayMapEntry aEntry)
	{
		int low = 0;
		int high = mEntryCount - 1;

		while (low <= high)
		{
			int mid = (low + high) >>> 1;

			int entryOffset = readEntryOffset(mid);
			int keyOffset = readKeyOffset(entryOffset);
			int keyLength = readKeyLength(entryOffset);

			int cmp = aEntry.compareToIncludingType(mBuffer, mStartOffset + keyOffset, keyLength);

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


	int readEntryOffset(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mEntryCount : aIndex;

		return readInt32(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE);
	}


	private void writeEntryOffset(int aIndex, int aEntryOffset)
	{
		assert aIndex >= 0 && aIndex < mEntryCount;
		assert aEntryOffset > 0 && aEntryOffset < mCapacity;

		writeInt32(mPointerListOffset + aIndex * ENTRY_POINTER_SIZE, aEntryOffset);
	}


	int readKeyOffset(int aEntryOffset)
	{
		return aEntryOffset + ENTRY_HEADER_SIZE;
	}


	int readKeyLength(int aEntryOffset)
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
		return getInt16(mBuffer, mStartOffset + aOffset);
	}


	private void writeInt16(int aOffset, int aValue)
	{
		putInt16(mBuffer, mStartOffset + aOffset, aValue);
	}


	private int readInt32(int aOffset)
	{
		return getInt32(mBuffer, mStartOffset + aOffset);
	}


	private void writeInt32(int aOffset, int aValue)
	{
		putInt32(mBuffer, mStartOffset + aOffset, aValue);
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
		ArrayMapEntry prevKey = null;

		for (int i = 0; i < mEntryCount; i++)
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

			ArrayMapEntry nextKey = new ArrayMapEntry().setMarshalledKey(mBuffer, mStartOffset + keyOffset, keyLength);

			if (prevKey != null && prevKey.compareTo(nextKey) >= 0)
			{
				return "Keys are out of order: " + getKey(i - 1).toKeyString() + ", " + getKey(i).toKeyString();
			}
			prevKey = nextKey;
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
//		try
		{
			StringBuilder sb = new StringBuilder();
			for (ArrayMapEntry entry : this)
			{
				if (sb.length() > 0)
				{
					sb.append(",");
				}
//				sb.append("\"" + new String(entry.getKey().array(), "utf-8").replaceAll("[^\\w]*", "") + "\"");
				sb.append("\"" + entry.toKeyString() + "\"");
			}
			return sb.insert(0, "[").append("]").toString();
		}
//		catch (UnsupportedEncodingException e)
//		{
//			throw new IllegalStateException(e);
//		}
	}


	@Override
	public ArrayMapEntryIterator iterator()
	{
		return new ArrayMapEntryIterator(this);
	}


	public ArrayMapEntry getFirst()
	{
		return mEntryCount == 0 ? null : get(0);
	}


	public ArrayMapEntry getLast()
	{
		return mEntryCount == 0 ? null : get(mEntryCount - 1);
	}


	public ArrayMapEntry removeFirst()
	{
		ArrayMapEntry entry = getFirst();
		removeImpl(0);
		return entry;
	}


	public ArrayMapEntry removeLast()
	{
		ArrayMapEntry entry = getLast();
		removeImpl(0);
		return entry;
	}


	public ArrayMap resize(int aNewCapacity)
	{
		if (aNewCapacity < mCapacity && mCapacity - aNewCapacity > getFreeSpace())
		{
			throw new IllegalArgumentException();
		}

		int s = ENTRY_POINTER_SIZE * mEntryCount;
		byte[] buffer = new byte[aNewCapacity];

		System.arraycopy(mBuffer, 0, buffer, 0, mFreeSpaceOffset);
		System.arraycopy(mBuffer, mBuffer.length - s, buffer, aNewCapacity - s, s);

		mBuffer = buffer;
		mCapacity = buffer.length;
		mPointerListOffset = mCapacity - ENTRY_POINTER_SIZE * mEntryCount;

		return this;
	}


	public ArrayMap[] splitHalf(int aCapacity)
	{
		ArrayMap low = new ArrayMap(aCapacity, mCapacityGrowth);
		ArrayMap high = new ArrayMap(aCapacity, mCapacityGrowth);

		for (int i = 0, j = mEntryCount; i < j;)
		{
			if (low.getFreeSpace() > high.getFreeSpace())
			{
				low.insert(get(i++));
			}
			else
			{
				high.insert(get(--j));
			}
		}

		return new ArrayMap[]
		{
			low, high
		};
	}

	// ----------
	// ----------
	// ----------
	// ----------
	// ----------
	// ----------

	// ----------
	// ----------
	// ----------
	//                      ----------
	//                      ----------
	//                      ----------
	// ----------
	// ----------
	//            ----------
	//            ----------
	//                      ----------
	//                      ----------
	public ArrayMap[] splitMany(int aCapacity)
	{
		ArrayList<ArrayMap> maps = new ArrayList<>();

		int bytesPerNode = getUsedSpace() / ((getUsedSpace() + aCapacity - 1) / aCapacity);

		ArrayMap map = new ArrayMap(aCapacity, mCapacityGrowth);
		maps.add(map);

		for (int i = 0; i < mEntryCount; i++)
		{
			ArrayMapEntry entry = get(i);

			if (map.getUsedSpace() + entry.getMarshalledLength() > bytesPerNode && i < mEntryCount - 2)
			{
				map = new ArrayMap(aCapacity, mCapacityGrowth);
				maps.add(map);
			}

			map.insert(entry);
		}

		return maps.toArray(ArrayMap[]::new);
	}


	public ArrayMap[] splitManyTail(int aCapacity)
	{
		ArrayList<ArrayMap> maps = new ArrayList<>();

		ArrayMap map = new ArrayMap(aCapacity, mCapacityGrowth);
		maps.add(map);

		for (int i = 0; i < mEntryCount; i++)
		{
			ArrayMapEntry entry = get(i);

			map.put(entry);

			if (entry.getState() == OpState.OVERFLOW)
			{
				map = new ArrayMap(aCapacity, mCapacityGrowth);
				maps.add(map);
				map.insert(get(i));
			}
		}

		return maps.toArray(ArrayMap[]::new);
	}


	public boolean isHalfEmpty()
	{
		return getFreeSpace() > mCapacity / 2;
	}


	String keys(Function<Object, String> aFormatter)
	{
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size(); i++)
		{
			if (i > 0)
			{
				sb.append(", ");
			}
			sb.append(aFormatter.apply(getKey(i)));
		}
		return sb.toString();
	}
}
