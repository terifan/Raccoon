package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.storage.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;


final class IndexNode implements Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private final byte[] mBuffer;
	private final int mPointerCount;


	public IndexNode(byte[] aBuffer)
	{
		mPointerCount = aBuffer.length / BlockPointer.SIZE;
		mBuffer = aBuffer;

		assert PerformanceCounters.increment(INDEX_NODE_CREATION);
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.NODE_INDEX;
	}


	int getPointerCount()
	{
		return mPointerCount;
	}


	void setPointer(int aIndex, BlockPointer aBlockPointer)
	{
		assert get(aIndex).getType() == BlockType.NODE_FREE || get(aIndex).getRange() == aBlockPointer.getRange() : get(aIndex).getType() + " " + get(aIndex).getRange() +"=="+ aBlockPointer.getRange();

		set(aIndex, aBlockPointer);
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.NODE_FREE)
		{
			return null;
		}

		BlockPointer blockPointer = get(aIndex);

		assert blockPointer.getRange() != 0;

		return blockPointer;
	}


	int findPointer(int aIndex)
	{
		for (; getPointerType(aIndex) == BlockType.NODE_FREE; aIndex--)
		{
		}

		return aIndex;
	}


	void split(int aIndex, BlockPointer aLowPointer, BlockPointer aHighPointer)
	{
		assert aLowPointer.getRange() + aHighPointer.getRange() == get(aIndex).getRange();
		assert ensureEmpty(aIndex + 1, aLowPointer.getRange() + aHighPointer.getRange() - 1);

		set(aIndex, aLowPointer);
		set(aIndex + aLowPointer.getRange(), aHighPointer);
	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = get(aIndex);
		BlockPointer bp2 = get(aIndex + aBlockPointer.getRange());

		assert bp1.getRange() + bp2.getRange() == aBlockPointer.getRange();
		assert ensureEmpty(aIndex + 1, bp1.getRange() - 1);
		assert ensureEmpty(aIndex + bp1.getRange() + 1, bp2.getRange() - 1);

		set(aIndex, aBlockPointer);
		set(aIndex + bp1.getRange(), EMPTY_POINTER);
	}


	private boolean ensureEmpty(int aIndex, int aRange)
	{
		byte[] array = mBuffer;

		for (int i = aIndex * BlockPointer.SIZE, limit = (aIndex + aRange) * BlockPointer.SIZE; i < limit; i++)
		{
			if (array[i] != 0)
			{
				return false;
			}
		}

		return true;
	}


	private BlockType getPointerType(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		return BlockPointer.getType(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private BlockPointer get(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		return new BlockPointer().unmarshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	private void set(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		aBlockPointer.marshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	String integrityCheck()
	{
		int rangeRemain = 0;

		for (int i = 0; i < mPointerCount; i++)
		{
			BlockPointer bp = get(i);

			if (rangeRemain > 0)
			{
				if (bp.getRange() != 0)
				{
					return "Pointer inside range";
				}
			}
			else
			{
				if (bp.getRange() == 0)
				{
					return "Zero range";
				}
				rangeRemain = bp.getRange();
			}
			rangeRemain--;
		}

		return null;
	}
}
