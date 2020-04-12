package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableRootNode implements HashTableNode
{
	private HashTableNode mInstance;
	private BlockPointer mBlockPointer;
	private HashTable mHashTable;


	HashTableRootNode(HashTable aHashTable, boolean aCreate)
	{
		mHashTable = aHashTable;

		if (aCreate)
		{
			mInstance = new HashTableLeafNode(mHashTable, null);
			mBlockPointer = mHashTable.writeBlock(mInstance, mHashTable.mPointersPerNode);
		}
	}


	public void loadRoot()
	{
		Log.i("load root %s", mBlockPointer);

		if (mBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mInstance = new HashTableLeafNode(mHashTable, null, mBlockPointer);
		}
		else
		{
			mInstance = new HashTableInnerNode(mHashTable, mBlockPointer);
		}
	}


	void marshal(ByteArrayBuffer aBuffer)
	{
		mBlockPointer.marshal(aBuffer);
	}


	void unmarshal(ByteArrayBuffer aBuffer)
	{
		mBlockPointer = new BlockPointer();
		mBlockPointer.unmarshal(aBuffer);
	}


	Iterator<ArrayMapEntry> iterator()
	{
		return new HashTableNodeIterator(mHashTable, mInstance);
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	void writeBlock()
	{
		mHashTable.freeBlock(mBlockPointer);

		mBlockPointer = mHashTable.writeBlock(mInstance, mHashTable.mPointersPerNode);
	}


	void rollback(boolean aWasEmptyInstance)
	{
		mInstance = null;

		if (aWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mInstance = new HashTableLeafNode(mHashTable, null);
		}
		else
		{
			Log.d("rollback %s", mBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


	void close()
	{
		mInstance = null;
	}


	@Override
	public String integrityCheck()
	{
		return mInstance.integrityCheck();
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		mInstance.visit(aVisitor);
	}


	@Override
	public byte[] array()
	{
		return mInstance.array();
	}


	@Override
	public BlockType getType()
	{
		return mInstance.getType();
	}


	@Override
	public boolean getValue(ArrayMapEntry aEntry, long aHash, int aLevel)
	{
		return mInstance.getValue(aEntry, aHash, aLevel);
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		if (mInstance instanceof HashTableLeafNode)
		{
			Log.d("put root value");

			if (mInstance.putValue(aEntry, oOldEntry, aHash, 0))
			{
				return true;
			}

			Log.d("upgrade root from leaf to node");

			mInstance = ((HashTableLeafNode)mInstance).splitLeaf(0);

			mBlockPointer = mHashTable.writeBlock(mInstance, mHashTable.mPointersPerNode);
		}

		return mInstance.putValue(aEntry, oOldEntry, aHash, 0);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		return mInstance.removeValue(aEntry, oOldEntry, aHash, aLevel);
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		mInstance.scan(aScanResult);
	}
}
