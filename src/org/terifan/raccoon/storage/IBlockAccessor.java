package org.terifan.raccoon.storage;

import org.terifan.raccoon.BlockType;


public interface IBlockAccessor
{
	void freeBlock(BlockPointer aBlockPointer);


	byte[] readBlock(BlockPointer aBlockPointer);


	BlockPointer writeBlock(byte[] aBuffer, int aOffset, int aLength, long aTransactionId, BlockType aType, long aUserData);
}