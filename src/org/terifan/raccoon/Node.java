package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


interface Node
{
	BlockPointer getBlockPointer();


	byte[] array();


	BlockType getType();


	boolean getValue(ArrayMapEntry aEntry, long aHash, int aLevel);


	boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel);


	boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel);


	void scan(ScanResult aScanResult);


	void visit(HashTableVisitor aVisitor);


	String integrityCheck();
}
