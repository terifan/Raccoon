package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer;


interface Visitor
{
	int ROOT_POINTER = -1;

	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
