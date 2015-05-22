package org.terifan.raccoon.hashtable;


interface Visitor
{
	int ROOT_POINTER = -1;

	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
