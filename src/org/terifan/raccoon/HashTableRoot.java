package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class HashTableRoot implements Node
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


	public boolean get(ArrayMapEntry aEntry)
	{
		if (mRootMap != null)
		{
			return mRootMap.getValue(aEntry, 0);
		}

		return mRootNode.getValue(aEntry, 0);
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


	boolean remove(ArrayMapEntry aEntry)
	{
		boolean modified;
		if (mRootMap != null)
		{
			modified = mRootMap.remove(aEntry);
		}
		else
		{
			modified = mRootNode.removeValue(aEntry, aEntry.getKey(), 0);
		}
		return modified;
	}


	Iterator<ArrayMapEntry> iterator()
	{
		if (mRootNode != null)
		{
			return new HashTableNodeIterator(mHashTable, mRootBlockPointer);
		}
		if (!mRootMap.isEmpty())
		{
			return new HashTableNodeIterator(mHashTable, mRootMap);
		}

		return new ArrayList<ArrayMapEntry>().iterator();
	}


	public BlockPointer getRootBlockPointer()
	{
		return mRootBlockPointer;
	}


	void writeBlock()
	{
		mHashTable.freeBlock(mRootBlockPointer);

		if (mRootMap != null)
		{
			mRootBlockPointer = mHashTable.writeBlock(mRootMap, mHashTable.mPointersPerNode);
		}
		else
		{
			mRootBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
		}
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


	void clear()
	{
		if (mRootMap != null)
		{
			mRootMap.clear();
		}
		else
		{
			mHashTable.visit((aPointerIndex, aBlockPointer) ->
			{
				mHashTable.mCost.mTreeTraversal++;

				if (aPointerIndex >= 0 && aBlockPointer != null && (aBlockPointer.getBlockType() == BlockType.INDEX || aBlockPointer.getBlockType() == BlockType.LEAF))
				{
					mHashTable.freeBlock(aBlockPointer);
				}
			});

			mRootNode = null;
			mRootMap = new HashTableLeaf(mHashTable, null);
		}

		mHashTable.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mHashTable.writeBlock(mRootMap, mHashTable.mPointersPerNode);
	}


	void close()
	{
		mRootMap = null;
		mRootNode = null;
	}


	public String integrityCheck()
	{
		Log.i("integrity check");

		if (mRootMap != null)
		{
			return mRootMap.integrityCheck();
		}

		return mRootNode.integrityCheck();
	}


	void visit(HashTableVisitor aVisitor)
	{
		if (mRootNode != null)
		{
			mRootNode.visitNode(aVisitor);
		}

		aVisitor.visit(-1, mRootBlockPointer); // start visit at root level
	}


	@Override
	public byte[] array()
	{
		return mRootNode != null ? mRootNode.array() : mRootMap.array();
	}


	@Override
	public BlockType getType()
	{
		return mRootNode != null ? BlockType.INDEX : BlockType.LEAF;
	}


	@Override
	public boolean getValue(ArrayMapEntry aEntry, int aLevel)
	{
		return mRootNode != null ? mRootNode.getValue(aEntry, aLevel) : mRootMap.getValue(aEntry, aLevel);
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		return mRootNode != null ? mRootNode.putValue(aEntry, aKey, aLevel) : mRootMap.putValue(aEntry, aKey, aLevel);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		return mRootNode != null ? mRootNode.removeValue(aEntry, aKey, aLevel) : mRootMap.removeValue(aEntry, aKey, aLevel);
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		mRootNode.scan(aScanResult);
	}
}
