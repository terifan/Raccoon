package org.terifan.raccoon;


public interface BlockType
{
	int FREE = 0;
	int HOLE = 1;
	int TREE_INTERIOR_NODE = 2;
	int TREE_LEAF_NODE = 3;
	int BLOB_INTERIOR_NODE = 4;
	int BLOB_LEAF_NODE = 5;
	int SPACEMAP = 6;
	int ILLEGAL = 7;
	int APPLICATION_HEADER = 8;
}
