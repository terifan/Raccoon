package org.terifan.raccoon.hashtable;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.terifan.raccoon.Node;
import static org.terifan.raccoon.Node.*;
import org.terifan.raccoon.Stats;


/*
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |   type  |   comp  |              x              |       range       |pagecount|
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |                                     offset                                    |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |                                   transaction                                 |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |                checksum               |               block key               |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 *
 *   8 type (unallocated, hole, index, leaf)
 *   8 compression
 *  24 unused
 *  16 range
 *   8 page count
 *  64 offset
 *  64 transaction id
 *  32 checksum
 *  32 block key
 *
 */
public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 32;

	private int mType;
	private int mCompression;
	private int mRange;
	private int mPageCount;
	private long mPageIndex;
	private long mTransactionId;
	private int mChecksum;
	private int mBlockKey;


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


	public long getPageIndex()
	{
		return mPageIndex;
	}


	public BlockPointer setPageIndex(long aPageIndex)
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


	public int getBlockKey()
	{
		return mBlockKey;
	}


	public BlockPointer setBlockKey(int aBlockKey)
	{
		mBlockKey = aBlockKey;
		return this;
	}


	public long getTransactionId()
	{
		return mTransactionId;
	}


	public BlockPointer setTransactionId(long aTransactionId)
	{
		mTransactionId = aTransactionId;
		return this;
	}


	public byte[] encode(byte[] aBuffer, int aOffset)
	{
		assert mType >= 1 && mType <= 255 : mType;
		assert mCompression >= 0 && mCompression <= 255 : mCompression;
		assert mType == Node.HOLE || mPageCount >= 1 && mPageCount <= 256 : mPageCount;
		assert mRange >= 1 && mRange <= 4096 : mRange;

		ByteBuffer bb = ByteBuffer.wrap(aBuffer);
		bb.position(aOffset);
		bb.put((byte)mType);
		bb.put((byte)mCompression);
		bb.put((byte)0);
		bb.put((byte)0);
		bb.put((byte)0);
		bb.putShort((short)mRange);
		bb.put((byte)(mType == Node.HOLE ? 0 : (mPageCount - 1)));
		bb.putLong(mPageIndex);
		bb.putLong(mTransactionId);
		bb.putInt(mChecksum);
		bb.putInt(mBlockKey);

		Stats.pointerEncode++;

		return aBuffer;
	}


	public BlockPointer decode(byte[] aBuffer, int aOffset)
	{
		ByteBuffer bb = ByteBuffer.wrap(aBuffer);
		bb.position(aOffset);

		mType = 0xFF & bb.get();
		mCompression = 0xFF & bb.get();
		bb.get();
		bb.get();
		bb.get();
		mRange = 0xFFFF & bb.getShort();
		mPageCount = 0xFF & bb.get();
		mPageIndex = bb.getLong();
		mTransactionId = bb.getLong();
		mChecksum = bb.getInt();
		mBlockKey = bb.getInt();

		if (mType != Node.HOLE)
		{
			mPageCount++;
		}

		Stats.pointerDecode++;

		return this;
	}


	@Override
	public int hashCode()
	{
		return Long.hashCode(mPageIndex);
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
			&& aBlockPointer.mBlockKey == mBlockKey
			&& aBlockPointer.mPageCount == mPageCount
			&& aBlockPointer.mChecksum == mChecksum
			&& aBlockPointer.mTransactionId == mTransactionId
			&& aBlockPointer.mCompression == mCompression
			&& aBlockPointer.mRange == mRange
			&& aBlockPointer.mType == mType;
	}


	@Override
	public String toString()
	{
		String t = "";

		switch (mType)
		{
			case LEAF: t = "LEAF"; break;
			case NODE: t = "NODE"; break;
			case HOLE: t = "HOLE"; break;
			case FREE: t = "FREE"; break;
		}

		return "{type=" + t + ", page=" + mPageIndex + ", count=" + mPageCount + ", range=" + mRange + ", tx=" + mTransactionId + ", blockKey=" + mBlockKey + ")";
	}
}