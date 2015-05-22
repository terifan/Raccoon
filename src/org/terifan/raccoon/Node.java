package org.terifan.raccoon;


public interface Node
{
	final static int FREE = 0;
	final static int HOLE = 1;
	final static int LEAF = 2;
	final static int NODE = 3;

	byte[] array();

	int getType();
}
