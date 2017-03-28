package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.storage.BlockPointer;


interface Visitor
{
	int ROOT_POINTER = -1;

	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
