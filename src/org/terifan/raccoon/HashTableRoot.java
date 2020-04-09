package org.terifan.raccoon;

import java.io.IOException;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class HashTableRoot implements Node
{
	HashTableLeaf mRootMap;
	HashTableNode mRootNode;
	BlockPointer mRootBlockPointer;
	private HashTable mHashTable;


	HashTableRoot(HashTable aHashTable, boolean aCreate)
	{
		mHashTable = aHashTable;

		if (aCreate)
		{
			mRootMap = new HashTableLeaf(mHashTable, null);
			mRootBlockPointer = mHashTable.writeBlock(mRootMap, mHashTable.mPointersPerNode);
		}
	}


	public void loadRoot()
	{
		Log.i("load root %s", mRootBlockPointer);

		if (mRootBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRootMap = new HashTableLeaf(mHashTable, null, mRootBlockPointer);
		}
		else
		{
			mRootNode = new HashTableNode(mHashTable, mRootBlockPointer);
		}
	}


	void marshal(ByteArrayBuffer aBuffer)
	{
		mRootBlockPointer.marshal(aBuffer);
	}


	void unmarshal(ByteArrayBuffer aBuffer)
	{
		mRootBlockPointer = new BlockPointer();
		mRootBlockPointer.unmarshal(aBuffer);
	}


	void put(ArrayMapEntry aEntry)
	{
		if (mRootMap != null)
		{
			Log.d("put root value");

			if (!mRootMap.put(aEntry))
			{
				Log.d("upgrade root leaf to node");

				mRootNode = mRootMap.splitLeaf(0);

				mRootBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			mRootNode.putValue(aEntry, aEntry.getKey(), 0);
		}
	}


	Iterator<ArrayMapEntry> iterator()
	{
		return new HashTableNodeIterator(mHashTable, node());
	}


	public BlockPointer getRootBlockPointer()
	{
		return mRootBlockPointer;
	}


	void writeBlock()
	{
		mHashTable.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mHashTable.writeBlock(node(), mHashTable.mPointersPerNode);
	}


	void rollback(boolean aWasEmptyInstance)
	{
		mRootNode = null;
		mRootMap = null;

		if (aWasEmptyInstance)
		{
			Log.d("rollback empty");

			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
			mRootMap = new HashTableLeaf(mHashTable, null);
		}
		else
		{
			Log.d("rollback %s", mRootBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

			loadRoot();
		}
	}


	void close()
	{
		mRootMap = null;
		mRootNode = null;
	}


	@Override
	public String integrityCheck()
	{
		Log.i("integrity check");

		return node().integrityCheck();
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		node().visit(aVisitor);
	}


	@Override
	public byte[] array()
	{
		return node().array();
	}


	@Override
	public BlockType getType()
	{
		return mRootNode != null ? BlockType.INDEX : BlockType.LEAF;
	}


	@Override
	public boolean getValue(ArrayMapEntry aEntry, int aLevel)
	{
		return node().getValue(aEntry, aLevel);
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		return node().putValue(aEntry, aKey, aLevel);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		return node().removeValue(aEntry, aKey, aLevel);
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return node().getBlockPointer();
	}


	Node node()
	{
		return mRootNode != null ? mRootNode : mRootMap;
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		mRootNode.scan(aScanResult);
	}
}
