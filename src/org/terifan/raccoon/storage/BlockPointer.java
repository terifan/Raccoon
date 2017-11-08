package org.terifan.raccoon.storage;

import java.io.Serializable;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;

	public final static int SIZE = 64;

	private int mBlockType;
	private int mLevel;
	private int mChecksumAlgorithm;
	private int mCompressionAlgorithm;
	private int mRangeOffset;
	private int mRangeSize;
	private int mAllocatedSize;
	private int mLogicalSize;
	private int mPhysicalSize;
	private int mBlockIndex0;
	private int mBlockIndex1;
	private int mBlockIndex2;
	private int mTransactionId;
	private long[] mChecksum;
	private long[] mIV;


	public BlockPointer()
	{
		mIV = new long[2];
		mChecksum = new long[2];
	}


	public int getLevel()
	{
		return mLevel;
	}


	public BlockPointer setLevel(int aLevel)
	{
		mLevel = aLevel;
		return this;
	}


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


	public int getAllocatedSize()
	{
		return mAllocatedSize;
	}


	public BlockPointer setAllocatedSize(int aAllocSize)
	{
		mAllocatedSize = aAllocSize;
		return this;
	}


	public int getLogicalSize()
	{
		return mLogicalSize;
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		mLogicalSize = aLogicalSize;
		return this;
	}


	public int getRangeSize()
	{
		return mRangeSize;
	}


	public BlockPointer setRangeSize(int aRangeSize)
	{
		mRangeSize = aRangeSize;
		return this;
	}


	public int getRangeOffset()
	{
		return mRangeOffset;
	}


	public BlockPointer setRangeOffset(int aRangeOffset)
	{
		mRangeOffset = aRangeOffset;
		return this;
	}


	public long getBlockIndex0()
	{
		return mBlockIndex0;
	}


	public BlockPointer setBlockIndex(long aBlockIndex)
	{
		mBlockIndex0 = (int)aBlockIndex;
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


	public long[] getChecksum()
	{
		return mChecksum.clone();
	}


	public BlockPointer setChecksum(long[] aChecksum)
	{
		mChecksum = aChecksum.clone();
		return this;
	}


	public long[] getIV()
	{
		return mIV.clone();
	}


	public BlockPointer setIV(long aIV0, long aIV1)
	{
		mIV[0] = aIV0;
		mIV[1] = aIV1;
		return this;
	}


	public BlockPointer setIV(long[] aIV)
	{
		mIV = aIV;
		return this;
	}


	public long getTransactionId()
	{
		return mTransactionId;
	}


	public BlockPointer setTransactionId(long aTransactionId)
	{
		mTransactionId = (int)aTransactionId;
		return this;
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		assert mBlockType >= 0 && mBlockType <= 0xff;
		assert mLevel >= 0 && mLevel <= 0xff;
		assert mCompressionAlgorithm >= 0 && mCompressionAlgorithm <= 0xff;
		assert mChecksumAlgorithm >= 0 && mChecksumAlgorithm <= 0x0f;
		assert mRangeOffset >= 0 && mRangeOffset <= 0xffff;
		assert mRangeSize >= 0 && mRangeSize <= 0xffff;
		assert mAllocatedSize >= 0 && mAllocatedSize <= 0xffff;
		assert mPhysicalSize >= 0 && mPhysicalSize <= 0xffffff;
		assert mLogicalSize >= 0 && mPhysicalSize <= 0xffffff;
		assert mBlockIndex0 >= 0 && mBlockIndex0 <= 0xffffffffL;
		assert mBlockIndex1 >= 0 && mBlockIndex1 <= 0xffffffffL;
		assert mBlockIndex2 >= 0 && mBlockIndex2 <= 0xffffffffL;
		assert mTransactionId >= 0 && mTransactionId <= 0xffffffffL;

		aBuffer.writeInt8(mBlockType);
		aBuffer.writeInt8(mLevel);
		aBuffer.writeInt8(mCompressionAlgorithm);
		aBuffer.writeInt8(mChecksumAlgorithm);
		aBuffer.writeInt16(mRangeOffset);
		aBuffer.writeInt16(mRangeSize);
		aBuffer.writeInt16(mAllocatedSize);
		aBuffer.writeInt24(mLogicalSize);
		aBuffer.writeInt24(mPhysicalSize);
		aBuffer.writeInt32(mBlockIndex0);
		aBuffer.writeInt32(mBlockIndex1);
		aBuffer.writeInt32(mBlockIndex2);
		aBuffer.writeInt32(mTransactionId);
		aBuffer.writeInt64(mIV[0]);
		aBuffer.writeInt64(mIV[1]);
		aBuffer.writeInt64(mChecksum[0]);
		aBuffer.writeInt64(mChecksum[1]);

		return aBuffer;
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		mBlockType = aBuffer.readInt8();
		mLevel = aBuffer.readInt8();
		mCompressionAlgorithm = aBuffer.readInt8();
		mChecksumAlgorithm = aBuffer.readInt8();
		mRangeOffset = aBuffer.readInt16();
		mRangeSize = aBuffer.readInt16();
		mAllocatedSize = aBuffer.readInt16();
		mLogicalSize = aBuffer.readInt24();
		mPhysicalSize = aBuffer.readInt24();
		mBlockIndex0 = aBuffer.readInt32();
		mBlockIndex1 = aBuffer.readInt32();
		mBlockIndex2 = aBuffer.readInt32();
		mTransactionId = aBuffer.readInt32();
		mIV[0] = aBuffer.readInt64();
		mIV[1] = aBuffer.readInt64();
		mChecksum[0] = aBuffer.readInt64();
		mChecksum[1] = aBuffer.readInt64();

		return this;
	}


	public boolean verifyChecksum(long[] aChecksum)
	{
		return aChecksum[0] == mChecksum[0] && aChecksum[1] == mChecksum[1];
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
		return BlockType.values()[0xFF & aBuffer[aOffset]];
	}


	@Override
	public int hashCode()
	{
		return (int)(mBlockIndex0 ^ (mBlockIndex0 >>> 32));
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getBlockIndex0() == mBlockIndex0;
		}
		return false;
	}


	@Override
	public String toString()
	{
		return "{type=" + getBlockType() + ", level=" + mLevel + ", offset=[" + mBlockIndex0+","+mBlockIndex1+","+mBlockIndex2 + "], alloc=" + mAllocatedSize + ", phys=" + mPhysicalSize + ", logic=" + mLogicalSize + ", range=" + mRangeOffset + ":" + mRangeSize + ", tx=" + mTransactionId + ")";
	}
}