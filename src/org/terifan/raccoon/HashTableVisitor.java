package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


interface HashTableVisitor
{
	void visit(int aPointerIndex, BlockPointer aBlockPointer);
}
