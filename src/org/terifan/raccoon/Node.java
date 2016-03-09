package org.terifan.raccoon;

import org.terifan.raccoon.io.BlockPointer.BlockType;


public interface Node
{
	byte[] array();

	BlockType getType();
}
