package org.terifan.raccoon.hashtable;

import org.terifan.raccoon.storage.BlockType;


interface Node
{
	byte[] array();

	BlockType getType();
}
