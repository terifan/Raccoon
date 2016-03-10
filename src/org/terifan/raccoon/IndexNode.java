package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer;
import org.terifan.raccoon.io.BlockPointer.BlockType;


class IndexNode implements Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private final byte[] mBuffer;
	private final int mPointerCount;


	public IndexNode(byte[] aBuffer)
	{
		mPointerCount = aBuffer.length / BlockPointer.SIZE;
		mBuffer = aBuffer;

		Stats.indexNodeCreation.incrementAndGet();
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.NODE_INDX;
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

		return new BlockPointer().unmarshal(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private void set(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		aBlockPointer.marshal(mBuffer, aIndex * BlockPointer.SIZE);
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


//	public void dump()
//	{
//		StringBuilder sb = new StringBuilder();
//
//		for (int i = 0; i < mBuffer.length / BlockPointer.SIZE;)
//		{
//			BlockPointer bp = decodePointer(i);
//
//			if (bp.getRange() == 0)
//			{
//				throw new IllegalArgumentException();
//			}
//
//			sb.append(String.format("%3d: ", i));
//			sb.append(bp);
//			sb.append("\n");
//
//			for (int j = 0; j < bp.getRange()-1; j++)
//			{
//				sb.append(String.format("%3d: ", i+1+j));
//				sb.append("------------\n");
//			}
//
//			i += bp.getRange();
//		}
//
//		System.out.println(sb.toString());
//	}
}
