package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;


interface Node
{
	BlockPointer getBlockPointer();


	byte[] array();


	BlockType getType();


	boolean getValue(ArrayMapEntry aEntry, int aLevel);


	boolean putValue(ArrayMapEntry aEntry, int aLevel);


	boolean removeValue(ArrayMapEntry aEntry, int aLevel);


	void scan(ScanResult aScanResult);


	void visit(HashTableVisitor aVisitor);


	String integrityCheck();
}
