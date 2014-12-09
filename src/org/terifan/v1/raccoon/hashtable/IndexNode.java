package org.terifan.v1.raccoon.hashtable;

import org.terifan.v1.raccoon.Node;
import java.util.Arrays;
import org.terifan.v1.raccoon.Stats;
import org.terifan.v1.util.Log;


public class IndexNode implements Node
{
	private byte[] mBuffer;


	private IndexNode()
	{
		Stats.indexNodeCreation++;
	}


	public static IndexNode wrap(byte[] aBuffer)
	{
		IndexNode node = new IndexNode();
		node.mBuffer = aBuffer;
		return node;
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	public void setPointer(int aIndex, BlockPointer aBlockPointer)
	{
		assert decodePointer(aIndex).getType() == 0 || decodePointer(aIndex).getRange() == aBlockPointer.getRange();

		aBlockPointer.encode(mBuffer, BlockPointer.SIZE * aIndex);
	}


	public BlockPointer getPointer(int aIndex)
	{
		if (aIndex < 0 || aIndex >= getPointerCount())
		{
			throw new IllegalArgumentException("Illegal index " + aIndex);
		}

		BlockPointer decoded = decodePointer(aIndex);

		if (decoded.getType() == 0)
		{
			return null;
		}
		if (decoded.getRange() == 0)
		{
			throw new IllegalArgumentException("Bad block pointer at index: " + aIndex + ", pointer: " + decoded);
		}

		return decoded;
	}


	private BlockPointer decodePointer(int aIndex)
	{
		return new BlockPointer().decode(mBuffer, BlockPointer.SIZE * aIndex);
	}


	public int findPointer(int aIndex)
	{
		for (; mBuffer[aIndex * BlockPointer.SIZE] == 0; aIndex--)
		{
		}

		return aIndex;
	}


	public void split(int aIndex, BlockPointer aLowPointer, BlockPointer aHighPointer)
	{
		assert aLowPointer.getRange() + aHighPointer.getRange() == decodePointer(aIndex).getRange();

		ensureEmpty(aIndex + 1, aLowPointer.getRange() + aHighPointer.getRange() - 1);

		aLowPointer.encode(mBuffer, BlockPointer.SIZE * aIndex);
		aHighPointer.encode(mBuffer, BlockPointer.SIZE * (aIndex + aLowPointer.getRange()));
	}


	public void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = decodePointer(aIndex);
		BlockPointer bp2 = decodePointer(aIndex + aBlockPointer.getRange());

		assert bp1.getRange() + bp2.getRange() == aBlockPointer.getRange();

		ensureEmpty(aIndex + 1, bp1.getRange() - 1);
		ensureEmpty(aIndex + bp1.getRange() + 1, bp2.getRange() - 1);

		aBlockPointer.encode(mBuffer, BlockPointer.SIZE * aIndex);

		int offset = BlockPointer.SIZE * (aIndex + bp1.getRange());
		Arrays.fill(mBuffer, offset, offset + BlockPointer.SIZE, (byte)0);
	}


	private void ensureEmpty(int aIndex, int aRange)
	{
		for (int i = aIndex * BlockPointer.SIZE; i < (aIndex + aRange) * BlockPointer.SIZE; i++)
		{
			if (mBuffer[i] != 0)
			{
				throw new IllegalStateException();
			}
		}
	}


	public int getPointerCount()
	{
		return mBuffer.length / BlockPointer.SIZE;
	}


	String integrityCheck()
	{
		BlockPointer bp = new BlockPointer();
		int rangeRemain = 0;

		for (int i = 0; i < getPointerCount(); i++)
		{
			bp.decode(mBuffer, BlockPointer.SIZE * i);

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


	public void dump()
	{
		StringBuilder sb = new StringBuilder();

		BlockPointer bp = new BlockPointer();

		for (int i = 0; i < mBuffer.length / BlockPointer.SIZE;)
		{
			bp.decode(mBuffer, BlockPointer.SIZE * i);

			if (bp.getRange() == 0)
			{
				throw new IllegalArgumentException();
			}

			sb.append(String.format("%3d: ", i));
			sb.append(bp);
			sb.append("\n");

			for (int j = 0; j < bp.getRange()-1; j++)
			{
				sb.append(String.format("%3d: ", i+1+j));
				sb.append("------------\n");
			}

			i += bp.getRange();
		}

		Log.d(sb.toString());
	}


	@Override
	public int getType()
	{
		return Node.NODE;
	}
}
