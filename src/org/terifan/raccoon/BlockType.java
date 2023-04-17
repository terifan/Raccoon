package org.terifan.raccoon;


public interface BlockType
{
	int FREE = 0,
	HOLE = 1,
	TREE_INDEX = 2,
	TREE_LEAF = 3,
	BLOB_INDEX = 4,
	BLOB_LEAF = 5,
	SPACEMAP = 6,
	ILLEGAL = 7,
	APPLICATION_HEADER = 8;
}
