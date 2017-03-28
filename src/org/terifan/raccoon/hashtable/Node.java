package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlockPointer.BlockType;


interface Node
{
	byte[] array();

	BlockType getType();
}
