package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


class ExtendibleHashTableDirectory
{
	byte[] mBuffer;
	int mPrefixLength;


	ExtendibleHashTableDirectory(int aCapacity)
	{
		mBuffer = new byte[aCapacity];
		mPrefixLength = (int)Math.ceil(Math.log(mBuffer.length / BlockPointer.SIZE) / Math.log(2));
	}


	ExtendibleHashTableDirectory(BlockPointer aRootNodeBlockPointer, byte[] aBuffer)
	{
		mBuffer = aBuffer;
		mPrefixLength = (int)Math.ceil(Math.log(mBuffer.length / BlockPointer.SIZE) / Math.log(2));
	}


	int getPrefixLength()
	{
		return mPrefixLength;
	}


	BlockPointer writeBuffer(ExtendibleHashTableImplementation aImplementation, TransactionGroup mTransactionGroup)
	{
		return aImplementation.writeBlock(mTransactionGroup, mBuffer, BlockType.INDEX, 0L);
	}


	void setBlockPointer(int aIndex, BlockPointer aBlockPointer)
	{
		aBlockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	long getRangeBits(int aIndex)
	{
		return BlockPointer.readUserData(mBuffer, aIndex * BlockPointer.SIZE);
	}


	BlockPointer readBlockPointer(int aIndex)
	{
		BlockPointer bp = new BlockPointer();
		bp.unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
		return bp;
	}


	String integrityCheck()
	{
		for (int offset = 0; offset < mBuffer.length; )
		{
			BlockType blockType = BlockPointer.readBlockType(mBuffer, offset);

			if (blockType != BlockType.LEAF && blockType != BlockType.ILLEGAL)
			{
				return "ExtendibleHashTable directory has bad block type";
			}

			long rangeBits = BlockPointer.readUserData(mBuffer, offset);
			long range = BlockPointer.SIZE * (1 << rangeBits);

			if (range <= 0 || offset + range > mBuffer.length)
			{
				return "ExtendibleHashTable directory has bad range";
			}

			offset += BlockPointer.SIZE;
			range -= BlockPointer.SIZE;

			for (long j = range; --j >= 0; offset++)
			{
				if (mBuffer[offset] != 0)
				{
					return "ExtendibleHashTable directory error at " + offset;
				}
			}
		}

		return null;
	}
}
