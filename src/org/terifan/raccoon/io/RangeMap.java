package org.terifan.raccoon.io;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.util.ByteArrayBuffer;


class RangeMap implements Cloneable
{
	private TreeMap<Integer,Integer> mMap;
	private int mSpace;


	public RangeMap()
	{
		mMap = new TreeMap<>();
		mSpace = 0;
	}


	public void add(int aOffset, int aSize)
	{
		if (aOffset < 0 || aSize <= 0)
		{
			throw new IllegalArgumentException("Illegal range: offset: " + aOffset + ", size: " + aSize);
		}

		int start = aOffset;
		int end = aOffset + aSize;

		assert end > start : end+" > "+start;

		Integer before = mMap.floorKey(start);
		Integer after = mMap.ceilingKey(start);

		Integer v1 = mMap.lowerKey(end);
		Integer v2 = mMap.lowerKey(start);

		if (v1 != null && v1 >= start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (1): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}
		if (v2 != null && v2 < start && mMap.get(v2) > start)
		{
			throw new IllegalArgumentException("Offset overlap an existing region (2): offset: " + aOffset + ", size: " + aSize + ", existing start: " + v1 + ", existing end: " + mMap.get(v1));
		}

		boolean mergeBefore = (before != null && mMap.get(before) == start);
		boolean mergeAfter = (after != null && after == end);

		if (mergeBefore && mergeAfter)
		{
			mMap.put(before, mMap.remove(after));
		}
		else if (mergeBefore)
		{
			mMap.put(before, end);
		}
		else if (mergeAfter)
		{
			mMap.put(start, mMap.remove(after));
		}
		else
		{
			mMap.put(start, end);
		}

		mSpace += aSize;
	}


	public void remove(int aOffset, int aSize)
	{
		if (aSize <= 0)
		{
			throw new IllegalArgumentException("Size is zero or negative: size: " + aSize);
		}
		if (aOffset < 0)
		{
			throw new IllegalArgumentException("Offset is negative: offset: " + aOffset);
		}

		int start = aOffset;
		int end = aOffset + aSize;

		Integer blockStart = mMap.floorKey(start);

		if (blockStart == null)
		{
			throw new IllegalArgumentException("No free block at offset: offset: " + start);
		}

		int blockEnd = mMap.get(blockStart);

		if (end > blockEnd)
		{
			throw new IllegalArgumentException("Block size shorter than requested size: remove: " + start + "-" + end + ", from block: " + blockStart + "-" + blockEnd);
		}

		boolean leftOver = start != blockStart;
		boolean rightOver = end != blockEnd;

		if (leftOver && rightOver)
		{
			mMap.put(blockStart, start);
			mMap.put(end, blockEnd);
		}
		else if (leftOver)
		{
			mMap.put(blockStart, start);
		}
		else if (rightOver)
		{
			mMap.remove(blockStart);
			mMap.put(end, blockEnd);
		}
		else
		{
			mMap.remove(blockStart);
		}

		mSpace -= aSize;
	}


	public int next(int aSize)
	{
		Entry<Integer, Integer> entry = mMap.firstEntry();

		for (;;)
		{
			if (entry == null)
			{
				return -1;
			}

			int offset = entry.getKey();

			if (entry.getValue() - offset >= aSize)
			{
				remove(offset, aSize);

				return offset;
			}

			entry = mMap.higherEntry(offset);
		}
	}


	public int getFreeSpace()
	{
		return mSpace;
	}


	public int getUsedSpace()
	{
		return mMap.lastEntry().getValue() - mSpace;
	}


	public boolean isFree(int aOffset, int aSize)
	{
		Integer blockStart = mMap.floorKey(aOffset);

		if (blockStart != null)
		{
			int blockEnd = mMap.get(blockStart) - 1;

			if (blockEnd >= aOffset + aSize || blockEnd >= aOffset)
			{
				return false;
			}
		}

		return true;
	}


	public void clear()
	{
		mMap.clear();
		mSpace = 0;
	}


	@Override
	public RangeMap clone()
	{
		try
		{
			RangeMap map = (RangeMap)super.clone();
			map.mMap = (TreeMap<Integer,Integer>)this.mMap.clone();
			return map;
		}
		catch (CloneNotSupportedException e)
		{
			throw new RuntimeException(e);
		}
	}


	public void read(ByteArrayBuffer aDataInput) throws IOException
	{
		int size = aDataInput.readVar32();

		for (int i = 0, prev = 0; i < size; i++)
		{
			int count = aDataInput.readVar32();

			prev += aDataInput.readVar32();

			add(prev, count);
		}
	}


	public void write(ByteArrayBuffer aDataOutput) throws IOException
	{
		int prev = 0;

		aDataOutput.writeVar32(mMap.size());

		for (Entry<Integer,Integer> entry : mMap.entrySet())
		{
			int index = entry.getKey();

			aDataOutput.writeVar32(entry.getValue() - index);
			aDataOutput.writeVar32(index - prev);

			prev = index;
		}
	}


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder("{");
		for (Integer key : mMap.keySet())
		{
			if (sb.length() > 1)
			{
				sb.append(", ");
			}
			sb.append(key + "-" + (mMap.get(key) - 1));
		}
		sb.append("}");
		return sb.toString();
	}
}