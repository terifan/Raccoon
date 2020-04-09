package org.terifan.raccoon;


public interface Node
{
	byte[] array();

	BlockType getType();

	void scan(ScanResult aScanResult);

	boolean getValue(ArrayMapEntry aEntry, int aLevel);
}
