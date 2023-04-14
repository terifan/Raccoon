package org.terifan.raccoon.storage;

import java.io.Serializable;
import java.util.Arrays;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.BlockType;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.ByteArrayUtil;
import org.terifan.raccoon.util.Console;


public class BlockPointer implements Serializable
{
	private final static long serialVersionUID = 1;
	public final static int SIZE = 128;

	private final static int OFS_FLAG_TYPE = 0;
	private final static int OFS_FLAG_LEVEL = 1;
	private final static int OFS_FLAG_CHECKSUM = 2;
	private final static int OFS_FLAG_COMPRESSION = 3;
	private final static int OFS_ALLOCATED_SIZE = 4;
	private final static int OFS_LOGICAL_SIZE = 8;
	private final static int OFS_PHYSICAL_SIZE = 12;
	private final static int OFS_OFFSET0 = 16;
	private final static int OFS_OFFSET1 = 24;
	private final static int OFS_OFFSET2 = 32;
	private final static int OFS_TRANSACTION = 40;
	private final static int OFS_BLOCK_KEY = 48;
	private final static int OFS_CHECKSUM = 80;

	private byte[] mBuffer;


	public BlockPointer()
	{
		mBuffer = new byte[SIZE];
	}


	public BlockType getBlockType()
	{
		return BlockType.values()[mBuffer[OFS_FLAG_TYPE]];
	}


	public BlockPointer setBlockType(BlockType aBlockType)
	{
		mBuffer[OFS_FLAG_TYPE] = (byte)aBlockType.ordinal();
		return this;
	}


	public int getBlockLevel()
	{
		return 0xff & mBuffer[OFS_FLAG_LEVEL];
	}


	public BlockPointer setBlockLevel(int aLevel)
	{
		mBuffer[OFS_FLAG_LEVEL] = (byte)aLevel;
		return this;
	}


	/**
	 * Return the 'type' field from a BlockPointer stored in the buffer provided.
	 *
	 * @param aBuffer a buffer containing a BlockPointer
	 * @param aBlockPointerOffset start offset of the BlockPointer in the buffer
	 * @return the 'type' field
	 */
	public static BlockType readBlockType(byte[] aBuffer, int aBlockPointerOffset)
	{
		return BlockType.values()[0xFF & aBuffer[aBlockPointerOffset + OFS_FLAG_TYPE]];
	}


	public byte getChecksumAlgorithm()
	{
		return mBuffer[OFS_FLAG_CHECKSUM];
	}


	public BlockPointer setChecksumAlgorithm(byte aChecksumAlgorithm)
	{
		mBuffer[OFS_FLAG_CHECKSUM] = aChecksumAlgorithm;
		return this;
	}


	public int getCompressionAlgorithm()
	{
		return 0xff & mBuffer[OFS_FLAG_COMPRESSION];
	}


	public BlockPointer setCompressionAlgorithm(int aCompressionAlgorithm)
	{
		assert aCompressionAlgorithm >= 0 && aCompressionAlgorithm <= 255;
		mBuffer[OFS_FLAG_COMPRESSION] = (byte)aCompressionAlgorithm;
		return this;
	}


	public int getAllocatedSize()
	{
		return ByteArrayUtil.getInt32(mBuffer, OFS_ALLOCATED_SIZE);
	}


	public BlockPointer setAllocatedSize(int aAllocBlocks)
	{
		assert aAllocBlocks >= 0;

		ByteArrayUtil.putInt32(mBuffer, OFS_ALLOCATED_SIZE, aAllocBlocks);
		return this;
	}


	public int getLogicalSize()
	{
		return ByteArrayUtil.getInt32(mBuffer, OFS_LOGICAL_SIZE);
	}


	public BlockPointer setLogicalSize(int aLogicalSize)
	{
		ByteArrayUtil.putInt32(mBuffer, OFS_LOGICAL_SIZE, aLogicalSize);
		return this;
	}


	public int getPhysicalSize()
	{
		return ByteArrayUtil.getInt32(mBuffer, OFS_PHYSICAL_SIZE);
	}


	public BlockPointer setPhysicalSize(int aPhysicalSize)
	{
		ByteArrayUtil.putInt32(mBuffer, OFS_PHYSICAL_SIZE, aPhysicalSize);
		return this;
	}


	public int[] getBlockKey(int[] aBlockKey)
	{
		assert aBlockKey.length == 8;

		aBlockKey[0] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 0);
		aBlockKey[1] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 4);
		aBlockKey[2] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 8);
		aBlockKey[3] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 12);
		aBlockKey[4] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 16);
		aBlockKey[5] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 20);
		aBlockKey[6] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 24);
		aBlockKey[7] = ByteArrayUtil.getInt32(mBuffer, OFS_BLOCK_KEY + 28);
		return aBlockKey;
	}


	public BlockPointer setBlockKey(int... aBlockKey)
	{
		assert aBlockKey.length == 8;

		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 0, aBlockKey[0]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 4, aBlockKey[1]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 8, aBlockKey[2]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 12, aBlockKey[3]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 16, aBlockKey[4]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 20, aBlockKey[5]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 24, aBlockKey[6]);
		ByteArrayUtil.putInt32(mBuffer, OFS_BLOCK_KEY + 28, aBlockKey[7]);
		return this;
	}


	public long getBlockIndex0()
	{
		return ByteArrayUtil.getInt64(mBuffer, OFS_OFFSET0);
	}


	public BlockPointer setBlockIndex0(long aBlockIndex)
	{
		ByteArrayUtil.putInt64(mBuffer, OFS_OFFSET0, aBlockIndex);
		return this;
	}


	public long getBlockIndex1()
	{
		return ByteArrayUtil.getInt64(mBuffer, OFS_OFFSET1);
	}


	public BlockPointer setBlockIndex1(long aBlockIndex)
	{
		ByteArrayUtil.putInt64(mBuffer, OFS_OFFSET1, aBlockIndex);
		return this;
	}


	public long getBlockIndex2()
	{
		return ByteArrayUtil.getInt64(mBuffer, OFS_OFFSET2);
	}


	public BlockPointer setBlockIndex2(long aBlockIndex)
	{
		ByteArrayUtil.putInt64(mBuffer, OFS_OFFSET2, aBlockIndex);
		return this;
	}


	public long getTransactionId()
	{
		return ByteArrayUtil.getInt64(mBuffer, OFS_TRANSACTION);
	}


	public BlockPointer setTransactionId(long aTransactionId)
	{
		ByteArrayUtil.putInt64(mBuffer, OFS_TRANSACTION, aTransactionId);
		return this;
	}


	public long[] getChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		aChecksum[0] = ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 0);
		aChecksum[1] = ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 8);
		aChecksum[2] = ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 16);
		aChecksum[3] = ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 24);
		return aChecksum;
	}


	public BlockPointer setChecksum(long... aChecksum)
	{
		assert aChecksum.length == 4;

		ByteArrayUtil.putInt64(mBuffer, OFS_CHECKSUM + 0, aChecksum[0]);
		ByteArrayUtil.putInt64(mBuffer, OFS_CHECKSUM + 8, aChecksum[1]);
		ByteArrayUtil.putInt64(mBuffer, OFS_CHECKSUM + 16, aChecksum[2]);
		ByteArrayUtil.putInt64(mBuffer, OFS_CHECKSUM + 24, aChecksum[3]);
		return this;
	}


	public boolean verifyChecksum(long[] aChecksum)
	{
		assert aChecksum.length == 4;

		return aChecksum[0] == ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 0)
			&& aChecksum[1] == ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 8)
			&& aChecksum[2] == ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 16)
			&& aChecksum[3] == ByteArrayUtil.getInt64(mBuffer, OFS_CHECKSUM + 24);
	}


	public byte[] marshal()
	{
		return marshal(ByteArrayBuffer.alloc(SIZE)).array();
	}


	public BlockPointer unmarshal(byte[] aBinary)
	{
		return unmarshal(ByteArrayBuffer.wrap(aBinary));
	}


	public ByteArrayBuffer marshal(ByteArrayBuffer aBuffer)
	{
		return aBuffer.write(mBuffer);
	}


	public BlockPointer unmarshal(ByteArrayBuffer aBuffer)
	{
		aBuffer.read(mBuffer);
		return this;
	}


	public BlockPointer unmarshalDoc(Array aArray)
	{
		setBlockType(BlockType.values()[aArray.getInt(0)]);
		setBlockLevel(aArray.getInt(1));
		setCompressionAlgorithm(aArray.getInt(2));
		setAllocatedSize(aArray.getInt(3));
		setLogicalSize(aArray.getInt(4));
		setPhysicalSize(aArray.getInt(5));
		setTransactionId(aArray.getInt(6));
		setBlockIndex0(aArray.getArray(7).getLong(0));
		System.arraycopy(aArray.getBinary(8), 0, mBuffer, OFS_BLOCK_KEY, 32);
		System.arraycopy(aArray.getBinary(9), 0, mBuffer, OFS_CHECKSUM, 32);
		return this;
	}


	public Array marshalDoc()
	{
		return Array.of(
			getBlockType().ordinal(),
			getBlockLevel(),
			getCompressionAlgorithm(),
			getAllocatedSize(),
			getLogicalSize(),
			getPhysicalSize(),
			getTransactionId(),
			Array.of(getBlockIndex0()),
			Arrays.copyOfRange(mBuffer, OFS_BLOCK_KEY, OFS_BLOCK_KEY + 32),
			Arrays.copyOfRange(mBuffer, OFS_CHECKSUM, OFS_CHECKSUM + 32)
		);
	}


//	public BlockPointer unmarshalDoc(Document aDocument)
//	{
//		setBlockType(BlockType.values()[aDocument.getInt("type")]);
//		setBlockLevel(aDocument.getInt("lvl"));
//		setCompressionAlgorithm(aDocument.getInt("comp"));
//		setAllocatedSize(aDocument.getInt("alloc"));
//		setLogicalSize(aDocument.getInt("logic"));
//		setPhysicalSize(aDocument.getInt("phys"));
//		setTransactionId(aDocument.getInt("tx"));
//		setBlockIndex0(aDocument.getLongArray("blocks")[0]);
//		setBlockKey(aDocument.getLongArray("key"));
//		setChecksum(aDocument.getLongArray("chk"));
//		return this;
//	}
//
//
//	public Document marshalDoc()
//	{
//		Document doc = new Document()
//			.putNumber("type", getBlockType().ordinal())
//			.putNumber("lvl", getBlockLevel())
//			.putNumber("comp", getCompressionAlgorithm())
//			.putNumber("alloc", getAllocatedSize())
//			.putNumber("logic", getLogicalSize())
//			.putNumber("phys", getPhysicalSize())
//			.putNumber("tx", getTransactionId())
//			.putArray("blocks", Array.of(getBlockIndex0()))
//			.putArray("key", Array.of(getBlockKey(new long[4])))
//			.putArray("chk", Array.of(getChecksum(new long[4])))
//			;
//
//		return doc;
//	}


//	public BlockPointer unmarshalDoc(Document aDocument)
//	{
//		Array array = aDocument.getArray("");
//		setBlockType(BlockType.values()[array.getInt(0)]);
//		setBlockLevel(array.getInt(1));
//		setCompressionAlgorithm(array.getInt(2));
//		setAllocatedSize(array.getInt(3));
//		setLogicalSize(array.getInt(4));
//		setPhysicalSize(array.getInt(5));
//		setTransactionId(array.getInt(6));
//		setBlockIndex0(array.getArray(7).getLong(0));
//		setBlockKey(array.getArray(8).toLongs());
//		setChecksum(array.getArray(9).toLongs());
//		return this;
//	}
//
//
//	public Document marshalDoc()
//	{
//		Document doc = new Document()
//			.putArray("", Array.of(
//				getBlockType().ordinal(),
//				getBlockLevel(),
//				getCompressionAlgorithm(),
//				getAllocatedSize(),
//				getLogicalSize(),
//				getPhysicalSize(),
//				getTransactionId(),
//				Array.of(getBlockIndex0()),
//				Array.of(getBlockKey(new long[4])),
//				Array.of(getChecksum(new long[4]))
//			));
//
//		Log.hexDump(doc.marshal());
//
//		return doc;
//	}


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


	@Override
	public String toString()
	{
		return Console.format("{type=%s, offset=%d, alloc=%d, phys=%d, logic=%d, tx=%d}", getBlockType(), getBlockIndex0(), getAllocatedSize(), getPhysicalSize(), getLogicalSize(), getTransactionId());
	}
}
