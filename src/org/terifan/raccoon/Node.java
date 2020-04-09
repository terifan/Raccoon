package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.storage.BlockPointer;


interface Node
{
	BlockPointer getBlockPointer();

	byte[] array();

	BlockType getType();

	boolean getValue(ArrayMapEntry aEntry, int aLevel);

	boolean putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);

	boolean removeValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel);

	void scan(ScanResult aScanResult);

	String integrityCheck();

	void visit(HashTableVisitor aVisitor);
}
