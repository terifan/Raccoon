package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.storage.BlockPointer;


interface Visitor
{
	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
