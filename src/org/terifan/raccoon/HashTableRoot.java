package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableRoot implements Node
{
	private Node mRoot;
	private BlockPointer mBlockPointer;
	private HashTable mHashTable;


	HashTableRoot(HashTable aHashTable, boolean aCreate)
	{
		mHashTable = aHashTable;

		if (aCreate)
		{
			mRoot = new HashTableLeaf(mHashTable, null);
			mBlockPointer = mHashTable.writeBlock(mRoot, mHashTable.mPointersPerNode);
		}
	}


	public void loadRoot()
	{
		Log.i("load root %s", mBlockPointer);

		if (mBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRoot = new HashTableLeaf(mHashTable, null, mBlockPointer);
		}
		else
		{
			mRoot = new HashTableNode(mHashTable, mBlockPointer);
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
		return new HashTableNodeIterator(mHashTable, mRoot);
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	void writeBlock()
	{
		mHashTable.freeBlock(mBlockPointer);

		mBlockPointer = mHashTable.writeBlock(mRoot, mHashTable.mPointersPerNode);
	}


	void rollback(boolean aWasEmptyInstance)
	{
		mRoot = null;

		if (aWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRoot = new HashTableLeaf(mHashTable, null);
		}
		else
		{
			Log.d("rollback %s", mBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


	void close()
	{
		mRoot = null;
	}


	@Override
	public String integrityCheck()
	{
		return mRoot.integrityCheck();
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		mRoot.visit(aVisitor);
	}


	@Override
	public byte[] array()
	{
		return mRoot.array();
	}


	@Override
	public BlockType getType()
	{
		return mRoot.getType();
	}


	@Override
	public boolean getValue(ArrayMapEntry aEntry, int aLevel)
	{
		return mRoot.getValue(aEntry, aLevel);
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, int aLevel)
	{
		if (mRoot instanceof HashTableLeaf)
		{
			Log.d("put root value");

			if (mRoot.putValue(aEntry, oOldEntry, 0))
			{
				return true;
			}

			Log.d("upgrade root from leaf to node");

			mRoot = ((HashTableLeaf)mRoot).splitLeaf(0);

			mBlockPointer = mHashTable.writeBlock(mRoot, mHashTable.mPointersPerNode);
		}

		return mRoot.putValue(aEntry, oOldEntry, 0);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, int aLevel)
	{
		return mRoot.removeValue(aEntry, oOldEntry, aLevel);
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		mRoot.scan(aScanResult);
	}
}
