package org.terifan.raccoon.io;

import java.io.Serializable;
import org.terifan.raccoon.Stats;
import org.terifan.raccoon.util.ByteArrayBuffer;


/*
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |   type  |   comp  |       range       |              logical size             |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |            physical size              |                 offset                |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |                checksum               |               transaction             |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 * |                                    block key                                  |
 * +---------+---------+---------+---------+---------+---------+---------+---------+
 *
 *   8 type (2)
 *   8 compression (2)
 *  16 range (12)
 *  32 logical size (20)
 *  32 physical size (20)
 *  32 offset
 *  32 transaction id
 *  32 checksum
 *  64 block key
 */
public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 32;

	private int mType;
	private int mCompression;
	private int mRange;
	private int mLogicalSize;
	private int mPhysicalSize;
	private long mOffset;
	private long mTransactionId;
	private int mChecksum;
	private long mBlockKey;


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


	public int getLogicalSize()
	{
		return mLogicalSize;
	}


	public void setLogicalSize(int aLogicalSize)
	{
		this.mLogicalSize = aLogicalSize;
	}


	public BlockPointer setRange(int aRange)
	{
		mRange = aRange;
		return this;
	}


	public long getOffset()
	{
		return mOffset;
	}


	public BlockPointer setOffset(long aOffset)
	{
		mOffset = aOffset;
		return this;
	}


	public int getPhysicalSize()
	{
		return mPhysicalSize;
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		mPhysicalSize = aPhysicalSize;
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


	public long getBlockKey()
	{
		return mBlockKey;
	}


	public BlockPointer setBlockKey(long aBlockKey)
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


	public byte[] marshal(byte[] aBuffer, int aOffset)
	{
		return marshal(new ByteArrayBuffer(aBuffer).position(aOffset)).array();
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		assert mType >= 0 && mType < 256;
		assert mCompression >= 0 && mCompression < 256;
		assert mRange >= 0 && mRange < 65536;
		assert mPhysicalSize >= 0;
		assert mLogicalSize >= 0;
		assert mOffset >= 0 && mOffset < Integer.MAX_VALUE;
		assert mTransactionId >= 0 && mTransactionId < Integer.MAX_VALUE;

		aBuffer.write((byte)mType);
		aBuffer.write((byte)mCompression);
		aBuffer.writeInt16((short)mRange);
		aBuffer.writeInt32(mLogicalSize);
		aBuffer.writeInt32(mPhysicalSize);
		aBuffer.writeInt32((int)mOffset);
		aBuffer.writeInt32((int)mTransactionId);
		aBuffer.writeInt32(mChecksum);
		aBuffer.writeInt64(mBlockKey);

		Stats.pointerEncode++;

		return aBuffer;
	}


	public BlockPointer unmarshal(byte[] aBuffer, int aOffset)
	{
		return unmarshal(new ByteArrayBuffer(aBuffer).position(aOffset));
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		mType = aBuffer.read();
		mCompression = aBuffer.read();
		mRange = 0xFFFF & aBuffer.readInt16();
		mLogicalSize = aBuffer.readInt32();
		mPhysicalSize = aBuffer.readInt32();
		mOffset = 0xFFFFFFFFL & aBuffer.readInt32();
		mTransactionId = 0xFFFFFFFFL & aBuffer.readInt32();
		mChecksum = aBuffer.readInt32();
		mBlockKey = aBuffer.readInt64();

		Stats.pointerDecode++;

		return this;
	}


	@Override
	public int hashCode()
	{
		return (int)(mOffset ^ (mOffset >>> 32));
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getOffset() == mOffset;
		}
		return false;
	}


	@Override
	public String toString()
	{
		return "{type=" + mType + ", offset=" + mOffset + ", phys=" + mPhysicalSize + ", logic=" + mLogicalSize + ", range=" + mRange + ", tx=" + mTransactionId + ")";
	}
}