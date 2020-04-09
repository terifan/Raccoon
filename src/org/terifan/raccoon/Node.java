package org.terifan.raccoon;


public interface Node
{
	byte[] array();

	BlockType getType();

	void scan(ScanResult aScanResult);

	boolean getValue(ArrayMapEntry aEntry, int aLevel);

	boolean putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);

	boolean removeValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);
}
