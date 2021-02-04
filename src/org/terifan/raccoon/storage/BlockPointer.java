package org.terifan.raccoon.storage;

import java.io.Serializable;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ByteArrayUtil;


/*
 *   +------+------+------+------+------+------+------+------+
 * 0 | type | chk  | enc  | comp |      allocated blocks     |
 *   +------+------+------+------+------+------+------+------+
 * 1 |        logical size       |       physical size       |
 *   +------+------+------+------+------+------+------+------+
 * 2 |                      block index                      |
 *   +------+------+------+------+------+------+------+------+
 * 3 |                                                       |
 *   +------+------+------+------+------+------+------+------+
 * 4 |                      block index                      |
 *   +------+------+------+------+------+------+------+------+
 * 5 |                                                       |
 *   +------+------+------+------+------+------+------+------+
 * 6 |                      block index                      |
 *   +------+------+------+------+------+------+------+------+
 * 7 |                                                       |
 *   +------+------+------+------+------+------+------+------+
 * 8 |                       user data                       |
 *   +------+------+------+------+------+------+------+------+
 * 9 |                      transaction                      |
 *   +------+------+------+------+------+------+------+------+
 * A |                       block key                       |
 *   +------+------+------+------+------+------+------+------+
 * B |                       block key                       |
 *   +------+------+------+------+------+------+------+------+
 * C |                       checksum                        |
 *   +------+------+------+------+------+------+------+------+
 * D |                       checksum                        |
 *   +------+------+------+------+------+------+------+------+
 * E |                       checksum                        |
 *   +------+------+------+------+------+------+------+------+
 * F |                       checksum                        |
 *   +------+------+------+------+------+------+------+------+
 *
 *   8 block type
 *   8 checksum algorithm
 *   8 encryption algorithm
 *   8 compression algorithm
 *  32 allocated blocks
 *  32 logical size
 *  32 physical size
 * 128 block address
 *  64 user data
 *  64 transaction
 * 128 block key (initialization vector)
 * 256 checksum
 */
public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 128;

	private final static int OFS_FLAG_TYPE = 0;
	private final static int OFS_FLAG_CHECKSUM = 1;
	private final static int OFS_FLAG_ENCRYPTION = 2;
	private final static int OFS_FLAG_COMPRESSION = 3;
	private final static int OFS_ALLOCATED_BLOCKS = 4;
	private final static int OFS_LOGICAL_SIZE = 8;
	private final static int OFS_PHYSICAL_SIZE = 12;
	private final static int OFS_OFFSET1 = 32;
	private final static int OFS_USER_DATA = 64;
	private final static int OFS_TRANSACTION = 72;
	private final static int OFS_BLOCK_KEY = 80;
	private final static int OFS_CHECKSUM = 96;

	private byte[] mData;


	public BlockPointer()
	{
		mData = new byte[SIZE];
	}


	public BlockType getBlockType()
	{
		return BlockType.values()[mData[OFS_FLAG_TYPE]];
	}


	public BlockPointer setBlockType(BlockType aBlockType)
	{
		mData[OFS_FLAG_TYPE] = (byte)aBlockType.ordinal();
		return this;
	}


	/**
	 * Return the 'type' field from a BlockPointer stored in the buffer provided.
	 *
	 * @param aBuffer
	 *   a buffer containing a BlockPointer
	 * @param aBlockPointerOffset
	 *   start offset of the BlockPointer in the buffer
	 * @return
	 *   the 'type' field
	 */
	public static BlockType readBlockType(byte[] aBuffer, int aBlockPointerOffset)
	{
		return BlockType.values()[0xFF & aBuffer[aBlockPointerOffset + OFS_FLAG_TYPE]];
	}


	public byte getChecksumAlgorithm()
	{
		return mData[OFS_FLAG_CHECKSUM];
	}


	public BlockPointer setChecksumAlgorithm(byte aChecksumAlgorithm)
	{
		mData[OFS_FLAG_CHECKSUM] = aChecksumAlgorithm;
		return this;
	}


	public byte getEncryptionAlgorithm()
	{
		return mData[OFS_FLAG_ENCRYPTION];
	}


	public BlockPointer setEncryptionAlgorithm(byte aEncryptionAlgorithm)
	{
		mData[OFS_FLAG_ENCRYPTION] = aEncryptionAlgorithm;
		return this;
	}


	public byte getCompressionAlgorithm()
	{
		return mData[OFS_FLAG_COMPRESSION];
	}


	public BlockPointer setCompressionAlgorithm(byte aCompressionAlgorithm)
	{
		mData[OFS_FLAG_COMPRESSION] = aCompressionAlgorithm;
		return this;
	}


	public int getAllocatedBlocks()
	{
		return ByteArrayUtil.getInt32(mData, OFS_ALLOCATED_BLOCKS);
	}


	public BlockPointer setAllocatedBlocks(int aAllocBlocks)
	{
		ByteArrayUtil.putInt32(mData, OFS_ALLOCATED_BLOCKS, aAllocBlocks);
		return this;
	}


	public int getLogicalSize()
	{
		return ByteArrayUtil.getInt32(mData, OFS_LOGICAL_SIZE);
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		ByteArrayUtil.putInt32(mData, OFS_LOGICAL_SIZE, aLogicalSize);
		return this;
	}


	public int getPhysicalSize()
	{
		return ByteArrayUtil.getInt32(mData, OFS_PHYSICAL_SIZE);
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		ByteArrayUtil.putInt32(mData, OFS_PHYSICAL_SIZE, aPhysicalSize);
		return this;
	}


	public long[] getBlockKey(long[] aBlockKey)
	{
		assert aBlockKey.length == 2;

		aBlockKey[0] = ByteArrayUtil.getInt64(mData, OFS_BLOCK_KEY + 0);
		aBlockKey[1] = ByteArrayUtil.getInt64(mData, OFS_BLOCK_KEY + 8);
		return aBlockKey;
	}


	public BlockPointer setBlockKey(long[] aBlockKey)
	{
		assert aBlockKey.length == 2;

		ByteArrayUtil.putInt64(mData, OFS_BLOCK_KEY + 0, aBlockKey[0]);
		ByteArrayUtil.putInt64(mData, OFS_BLOCK_KEY + 8, aBlockKey[1]);
		return this;
	}


	public long getBlockIndex0()
	{
		return ByteArrayUtil.getInt64(mData, OFS_OFFSET1);
	}


	public BlockPointer setBlockIndex0(long aBlockIndex)
	{
		ByteArrayUtil.putInt64(mData, OFS_OFFSET1, aBlockIndex);
		return this;
	}


	public long getTransactionId()
	{
		return ByteArrayUtil.getInt64(mData, OFS_TRANSACTION);
	}


	public BlockPointer setTransactionId(long aTransactionId)
	{
		ByteArrayUtil.putInt64(mData, OFS_TRANSACTION, aTransactionId);
		return this;
	}


	public long[] getChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		aChecksum[0] = ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 0);
		aChecksum[1] = ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 8);
		aChecksum[2] = ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 16);
		aChecksum[3] = ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 24);
		return aChecksum;
	}


	public BlockPointer setChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		ByteArrayUtil.putInt64(mData, OFS_CHECKSUM + 0, aChecksum[0]);
		ByteArrayUtil.putInt64(mData, OFS_CHECKSUM + 8, aChecksum[1]);
		ByteArrayUtil.putInt64(mData, OFS_CHECKSUM + 16, aChecksum[2]);
		ByteArrayUtil.putInt64(mData, OFS_CHECKSUM + 24, aChecksum[3]);
		return this;
	}


	public boolean verifyChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		return aChecksum[0] == ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 0)
			&& aChecksum[1] == ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 8)
			&& aChecksum[2] == ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 16)
			&& aChecksum[3] == ByteArrayUtil.getInt64(mData, OFS_CHECKSUM + 24);
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		return aBuffer.write(mData);
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.read(mData);
		return this;
	}


	@Override
	public int hashCode()
	{
		return Long.hashCode(getBlockIndex0());
	}


	@Override
	public boolean equals(Object aBlockPointer)
	{
		if (aBlockPointer instanceof BlockPointer)
		{
			return ((BlockPointer)aBlockPointer).getBlockIndex0() == getBlockIndex0();
		}
		return false;
	}


	public long getUserData()
	{
		return ByteArrayUtil.getInt64(mData, OFS_USER_DATA);
	}


	public BlockPointer setUserData(long aUserData)
	{
		ByteArrayUtil.putInt64(mData, OFS_USER_DATA, aUserData);
		return this;
	}


	public static long readUserData(byte[] aBuffer, int aBlockPointerOffset)
	{
		return ByteArrayUtil.getInt64(aBuffer, aBlockPointerOffset + OFS_USER_DATA);
	}


	@Override
	public String toString()
	{
		return "{type=" + getBlockType() + ", offset=" + getBlockIndex0() + ", phys=" + getPhysicalSize() + ", logic=" + getLogicalSize() + ", tx=" + getTransactionId() + ")";
	}
}