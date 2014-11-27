package org.terifan.v1.raccoon.hashtable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.terifan.v1.raccoon.Node;
import org.terifan.v1.raccoon.Stats;


/*
 * +---------+---------+---------+---------+
 * |type|comp|xxxx|    range     |pagecount|
 * +---------+---------+---------+---------+
 * |                 offset                |
 * +---------+---------+---------+---------+
 * |              transaction              |
 * +---------+---------+---------+---------+
 * |                checksum               |
 * +---------+---------+---------+---------+
 *
 *   4 type (unallocated, hole, index, leaf)
 *   4 compression
 *   4 unused
 *  12 range
 *   8 page count
 *  32 offset
 *  32 transaction id
 *  32 checksum
 *
 */
public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 16;

	private int mType;
	private int mPageIndex;
	private int mPageCount;
	private int mRange;
	private int mCompression;
	private int mChecksum;
	private int mTransactionId;


	public int getCompression()
	{
		return mCompression;
	}


	public BlockPointer setCompression(int aCompression)
	{
		mCompression = aCompression;
		return this;
	}


	public int getType()
	{
		return mType;
	}


	public BlockPointer setType(int aType)
	{
		mType = aType;
		return this;
	}


	public int getRange()
	{
		return mRange;
	}


	BlockPointer setRange(int aRange)
	{
		if (aRange == 0)
		{
			throw new IllegalArgumentException("Range is zero");
		}

		mRange = aRange;
		return this;
	}


	public int getPageIndex()
	{
		return mPageIndex;
	}


	public BlockPointer setPageIndex(int aPageIndex)
	{
		mPageIndex = aPageIndex;
		return this;
	}


	public int getPageCount()
	{
		return mPageCount;
	}


	public BlockPointer setPageCount(int aPageCount)
	{
		mPageCount = aPageCount;
		return this;
	}


	public int getChecksum()
	{
		return mChecksum;
	}


	public BlockPointer setChecksum(int aChecksum)
	{
		mChecksum = aChecksum;
		return this;
	}


	public int getTransactionId()
	{
		return mTransactionId;
	}


	public BlockPointer setTransactionId(int aTransactionId)
	{
		mTransactionId = aTransactionId;
		return this;
	}


	public byte[] encode(byte[] aBuffer, int aOffset)
	{
		boolean hole = mType == Node.HOLE;

		assert mType >= 1 && mType <= 15 : mType;
		assert mCompression >= 0 && mCompression <= 15 : mCompression;
		assert hole || mPageCount >= 1 && mPageCount <= 256 : mPageCount;
		assert mRange >= 1 && mRange <= 4096 : mRange;

		int flags = (mType << 28) | (mCompression << 24) | ((mRange - 1) << 8) | (hole ? 0 : (mPageCount - 1));

		ByteBuffer bb = ByteBuffer.wrap(aBuffer);
		bb.position(aOffset);
		bb.putInt(flags);
		bb.putInt(mPageIndex);
		bb.putInt(mTransactionId);
		bb.putInt(mChecksum);

		Stats.pointerEncode++;
		
		return aBuffer;
	}


	public BlockPointer decode(byte[] aBuffer, int aOffset)
	{
		ByteBuffer bb = ByteBuffer.wrap(aBuffer);
		bb.position(aOffset);
		int flags = bb.getInt();
		mPageIndex = bb.getInt();
		mTransactionId = bb.getInt();
		mChecksum = bb.getInt();

		mType = 0xF & (flags >>> 28);
		mCompression = 0xF & (flags >>> 24);
		mRange = 0xFFF & (flags >>> 8);
		mPageCount = 0xFF & flags;

		if (mType == Node.LEAF || mType == Node.NODE)
		{
			mPageCount++;
		}
		if (mType != Node.UNALLOCATED)
		{
			mRange++;
		}

		Stats.pointerDecode++;

		return this;
	}


	@Override
	public int hashCode()
	{
		return mPageIndex;
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getPageIndex() == mPageIndex;
		}
		return false;
	}


	public boolean identical(BlockPointer aBlockPointer)
	{
		return aBlockPointer.mPageIndex == mPageIndex 
			&& aBlockPointer.mPageCount == mPageCount 
			&& aBlockPointer.mChecksum == mChecksum 
			&& aBlockPointer.mTransactionId == mTransactionId 
			&& aBlockPointer.mCompression == mCompression 
			&& aBlockPointer.mRange == mRange
			&& aBlockPointer.mType == mType;
	}


//	@Override
//	public String toString()
//	{
//		return ObjectSerializer.toString(this);
//	}
}