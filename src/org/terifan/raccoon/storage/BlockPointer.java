package org.terifan.raccoon.storage;

import org.terifan.raccoon.core.BlockType;
import java.io.Serializable;
import org.terifan.raccoon.PerformanceCounters;
import static org.terifan.raccoon.PerformanceCounters.*;
import org.terifan.raccoon.util.ByteArrayBuffer;


public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;

	public final static int SIZE = 64;

	private int mBlockType;
	private int mChecksumAlgorithm;
	private int mCompressionAlgorithm;
	private int mEncryptionAlgorithm;
	private int mRange;
	private int mAllocatedSize;
	private int mLogicalSize;
	private int mPhysicalSize;
	private long mBlockIndex;
	private long mTransactionId;
	private long mChecksum0;
	private long mChecksum1;
	private long mIV0;
	private long mIV1;


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


	public int getRange()
	{
		return mRange;
	}


	public BlockPointer setRange(int aRange)
	{
		mRange = aRange;
		return this;
	}


	public long getBlockIndex()
	{
		return mBlockIndex;
	}


	public BlockPointer setBlockIndex(long aBlockIndex)
	{
		mBlockIndex = aBlockIndex;
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


	public long getChecksum0()
	{
		return mChecksum0;
	}


	public BlockPointer setChecksum0(long aChecksum)
	{
		mChecksum0 = aChecksum;
		return this;
	}


	public long getChecksum1()
	{
		return mChecksum1;
	}


	public BlockPointer setChecksum1(long aChecksum)
	{
		mChecksum1 = aChecksum;
		return this;
	}


	public int getEncryptionAlgorithm()
	{
		return mEncryptionAlgorithm;
	}


	public BlockPointer setEncryptionAlgorithm(int aEncryptionAlgorithm)
	{
		mEncryptionAlgorithm = aEncryptionAlgorithm;
		return this;
	}


	public long getIV0()
	{
		return mIV0;
	}


	public BlockPointer setIV0(long aIV0)
	{
		mIV0 = aIV0;
		return this;
	}


	public long getIV1()
	{
		return mIV1;
	}


	public BlockPointer setIV1(long aIV1)
	{
		mIV1 = aIV1;
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
		assert mBlockType >= 0 && mBlockType < 256;
		assert mChecksumAlgorithm >= 0 && mChecksumAlgorithm < 256;
		assert mCompressionAlgorithm >= 0 && mCompressionAlgorithm < 256;
		assert mEncryptionAlgorithm >= 0 && mEncryptionAlgorithm < 256;
		assert mRange >= 0 && mRange < 65536;
		assert mAllocatedSize >= 0 && mAllocatedSize < 65536;
		assert mPhysicalSize >= 0;
		assert mLogicalSize >= 0;
		assert mBlockIndex >= 0;
		assert mTransactionId >= 0;

		aBuffer.writeInt8(mBlockType);
		aBuffer.writeInt8(mChecksumAlgorithm);
		aBuffer.writeInt8(mEncryptionAlgorithm);
		aBuffer.writeInt8(mCompressionAlgorithm);
		aBuffer.writeInt16(mRange);
		aBuffer.writeInt16(mAllocatedSize);
		aBuffer.writeInt32(mLogicalSize);
		aBuffer.writeInt32(mPhysicalSize);
		aBuffer.writeInt64(mBlockIndex);
		aBuffer.writeInt64(mTransactionId);
		aBuffer.writeInt64(mIV0);
		aBuffer.writeInt64(mIV1);
		aBuffer.writeInt64(mChecksum0);
		aBuffer.writeInt64(mChecksum1);

		assert PerformanceCounters.increment(POINTER_ENCODE);

		return aBuffer;
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		mBlockType = aBuffer.readInt8();
		mChecksumAlgorithm = aBuffer.readInt8();
		mEncryptionAlgorithm = aBuffer.readInt8();
		mCompressionAlgorithm = aBuffer.readInt8();
		mRange = aBuffer.readInt16();
		mAllocatedSize = aBuffer.readInt16();
		mLogicalSize = aBuffer.readInt32();
		mPhysicalSize = aBuffer.readInt32();
		mBlockIndex = aBuffer.readInt64();
		mTransactionId = aBuffer.readInt64();
		mIV0 = aBuffer.readInt64();
		mIV1 = aBuffer.readInt64();
		mChecksum0 = aBuffer.readInt64();
		mChecksum1 = aBuffer.readInt64();

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
		return BlockType.values()[0xFF & aBuffer[aOffset]];
	}


	@Override
	public int hashCode()
	{
		return (int)(mBlockIndex ^ (mBlockIndex >>> 32));
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getBlockIndex() == mBlockIndex;
		}
		return false;
	}


	@Override
	public String toString()
	{
		return "{type=" + getBlockType() + ", offset=" + mBlockIndex + ", phys=" + mPhysicalSize + ", logic=" + mLogicalSize + ", range=" + mRange + ", tx=" + mTransactionId + ")";
	}
}