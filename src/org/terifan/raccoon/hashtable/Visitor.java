package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlockPointer;


interface Visitor
{
	int ROOT_POINTER = -1;

	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
