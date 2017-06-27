package org.terifan.raccoon.storage;

import org.terifan.raccoon.core.BlockType;
import java.io.Serializable;
import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.util.ByteArrayBuffer;


/*
 * +------+------+------+------+------+------+------+------+
 * | type | chk  | enc  | comp |  X   |       range        |
 * |------+------+------+------+------+------+------+------+
 * |       physical size       |        logical size       |
 * |------+------+------+------+------+------+------+------+
 * |                         offset                        |
 * +------+------+------+------+------+------+------+------+
 * |                      transaction                      |
 * +------+------+------+------+------+------+------+------+
 * |                       checksum0                       |
 * +------+------+------+------+------+------+------+------+
 * |                       checksum1                       |
 * +------+------+------+------+------+------+------+------+
 * |                          iv0                          |
 * +------+------+------+------+------+------+------+------+
 * |                          iv1                          |
 * +------+------+------+------+------+------+------+------+
 *
 * +------+------+------+------+------+------+------+------+
 * |ve|typ|ch|cmp|    range    |        logical size       |
 * +------+------+------+------+------+------+------+------+
 * |       physical size       |           offset          |
 * +------+------+------+------+------+------+------+------+
 * |                      transaction                      |
 * +------+------+------+------+------+------+------+------+
 * |                       checksum                        |
 * +------+------+------+------+------+------+------+------+
 *
 *   4 version (1)
 *   4 block type (3)
 *   4 checksum algorithm (2)
 *   4 compression algorithm (3)
 *  16 range (12)
 *  32 logical size (20)
 *  32 physical size (20)
 *  40 offset
 *  64 transaction id
 *  64 checksum
 */
public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;

	private final static int VERSION = 0;
	public final static int SIZE = 32+32;

	private int mBlockType;
	private int mChecksumAlgorithm;
	private int mCompressionAlgorithm;
	private int mRange;
	private int mLogicalSize;
	private int mPhysicalSize;
	private long mOffset;
	private long mTransactionId;
	private long mChecksum;


	public int getChecksumAlgorithm()
	{
		return mChecksumAlgorithm;
	}


	public BlockPointer setChecksumAlgorithm(int aChecksumAlgorithm)
	{
		mChecksumAlgorithm = aChecksumAlgorithm;
		return this;
	}


	public int getCompressionAlgorithm()
	{
		return mCompressionAlgorithm;
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		mCompressionAlgorithm = aCompressionAlgorithm;
		return this;
	}


	public BlockType getBlockType()
	{
		return BlockType.values()[mBlockType];
	}


	public BlockPointer setBlockType(BlockType aBlockType)
	{
		mBlockType = aBlockType.ordinal();
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
		mLogicalSize = aLogicalSize;
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


	public long getChecksum()
	{
		return mChecksum;
	}


	public BlockPointer setChecksum(long aChecksum)
	{
		mChecksum = aChecksum;
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


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		assert mBlockType >= 0 && mBlockType < 16;
		assert mChecksumAlgorithm >= 0 && mChecksumAlgorithm < 16;
		assert mCompressionAlgorithm >= 0 && mCompressionAlgorithm < 16;
		assert mRange >= 0 && mRange < 65536;
		assert mPhysicalSize >= 0;
		assert mLogicalSize >= 0;
		assert mOffset >= 0 && mOffset < Integer.MAX_VALUE;
		assert mTransactionId >= 0 && mTransactionId < Integer.MAX_VALUE;

		aBuffer.writeInt8((VERSION << 4) + mBlockType);
		aBuffer.writeInt8((mChecksumAlgorithm << 4) + mCompressionAlgorithm);
		aBuffer.writeInt16(mRange);
		aBuffer.writeInt32(mLogicalSize);
		aBuffer.writeInt32(mPhysicalSize);
		aBuffer.writeInt32((int)mOffset);
		aBuffer.writeInt64(mTransactionId);

		aBuffer.writeInt64(0);
		aBuffer.writeInt64(0);
		aBuffer.writeInt64(mChecksum);
		aBuffer.writeInt64(mChecksum);

		aBuffer.writeInt64(mChecksum);

		assert PerformanceCounters.increment(POINTER_ENCODE);

		return aBuffer;
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		int vt = aBuffer.readInt8();

		switch (vt >> 4)
		{
			case 0:
				int cc = aBuffer.readInt8();

				mBlockType = 0x0F & vt;
				mChecksumAlgorithm = (cc >> 4);
				mCompressionAlgorithm = 0x0F & cc;
				mRange = aBuffer.readInt16();
				mLogicalSize = aBuffer.readInt32();
				mPhysicalSize = aBuffer.readInt32();
				mOffset = 0xFFFFFFFFL & aBuffer.readInt32();
				mTransactionId = aBuffer.readInt64();

				mChecksum = aBuffer.readInt64();
				mChecksum = aBuffer.readInt64();
				mChecksum = aBuffer.readInt64();
				mChecksum = aBuffer.readInt64();

				mChecksum = aBuffer.readInt64();
				break;
			default:
				throw new IllegalArgumentException("Unsupported BlockPointer serial format: " + (vt >> 4));
		}

		assert PerformanceCounters.increment(POINTER_DECODE);

		return this;
	}


	/**
	 * Return the 'type' field from a BlockPointer stored in the buffer provided.
	 *
	 * @param aBuffer
	 *   a buffer containing a BlockPointer
	 * @param aOffset
	 *   start offset of the BlockPointer in the buffer
	 * @return
	 *   the 'type' field
	 */
	public static BlockType getBlockType(byte[] aBuffer, int aOffset)
	{
		return BlockType.values()[0x0F & aBuffer[aOffset]];
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
		return "{type=" + getBlockType() + ", offset=" + mOffset + ", phys=" + mPhysicalSize + ", logic=" + mLogicalSize + ", range=" + mRange + ", tx=" + mTransactionId + ")";
	}
}