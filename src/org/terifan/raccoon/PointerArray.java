package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


public class PointerArray
{
	private BlockPointer[] mBuffer;

	PointerArray(int aSize)
	{
		mBuffer = new BlockPointer[aSize];
	}


	BlockPointer get(int aIndex)
	{
		return mBuffer[aIndex];
	}


	void set(int aIndex, BlockPointer aBlockPointer)
	{
		mBuffer[aIndex] = aBlockPointer;
	}
}
