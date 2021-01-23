package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Result;


interface HashTableNode
{
	BlockPointer getBlockPointer();


	byte[] array();


	BlockType getType();


	boolean get(ArrayMapEntry aEntry, long aHash, int aLevel);


	boolean put(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel);


	boolean remove(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel);


	void clear();


	BlockPointer flush();


	void scan(ScanResult aScanResult);


	void visit(HashTableVisitor aVisitor);


	String integrityCheck();
}
