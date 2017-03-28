package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.io.BlockType;


interface Node
{
	byte[] array();

	BlockType getType();
}
