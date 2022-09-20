package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


class ExtendibleHashTableLeafNode
{
	BlockPointer mBlockPointer;
	ArrayMap mMap;
	long mRangeBits;
	boolean mChanged;
}
