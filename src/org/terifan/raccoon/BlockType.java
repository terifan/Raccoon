package org.terifan.raccoon;


public interface BlockType
{
	int FREE = 0;
	int HOLE = 1;
	int TREE_INDEX = 2;
	int TREE_LEAF = 3;
	int BLOB_INDEX = 4;
	int BLOB_LEAF = 5;
	int SPACEMAP = 6;
	int ILLEGAL = 7;
	int APPLICATION_HEADER = 8;
}
