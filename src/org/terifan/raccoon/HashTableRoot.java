package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


class HashTableRoot implements Node
{
	HashTableLeaf mRootMap;
	HashTableNode mRootNode;
	BlockPointer mBlockPointer;
	private HashTable mHashTable;


	HashTableRoot(HashTable aHashTable, boolean aCreate)
	{
		mHashTable = aHashTable;

		if (aCreate)
		{
			mRootMap = new HashTableLeaf(mHashTable, null);
			mBlockPointer = mHashTable.writeBlock(mRootMap, mHashTable.mPointersPerNode);
		}
	}


	public void loadRoot()
	{
		Log.i("load root %s", mBlockPointer);

		if (mBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRootMap = new HashTableLeaf(mHashTable, null, mBlockPointer);
		}
		else
		{
			mRootNode = new HashTableNode(mHashTable, mBlockPointer);
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


	void put(ArrayMapEntry aEntry)
	{
		if (mRootMap != null)
		{
			Log.d("put root value");

			if (!mRootMap.putValue(aEntry, 0))
			{
				Log.d("upgrade root leaf to node");

				mRootNode = mRootMap.splitLeaf(0);

				mBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			mRootNode.putValue(aEntry, 0);
		}
	}


	Iterator<ArrayMapEntry> iterator()
	{
		return new HashTableNodeIterator(mHashTable, node());
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	void writeBlock()
	{
		mHashTable.freeBlock(mBlockPointer);

		mBlockPointer = mHashTable.writeBlock(node(), mHashTable.mPointersPerNode);
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
			Log.d("rollback %s", mBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");

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
	public boolean putValue(ArrayMapEntry aEntry, int aLevel)
	{
		return node().putValue(aEntry, aLevel);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, int aLevel)
	{
		return node().removeValue(aEntry, aLevel);
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
