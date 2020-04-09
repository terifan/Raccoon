package org.terifan.raccoon;


public interface Node
{
	byte[] array();

	BlockType getType();

	boolean getValue(ArrayMapEntry aEntry, int aLevel);

	boolean putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);

	boolean removeValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);

	void scan(ScanResult aScanResult);

	String integrityCheck();
}
