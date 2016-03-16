package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer.BlockType;


interface Node
{
	byte[] array();

	BlockType getType();
}
