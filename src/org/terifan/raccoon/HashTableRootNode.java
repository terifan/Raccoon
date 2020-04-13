package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableRootNode //implements HashTableNode
{
//	private HashTableNode mRootNode;
//	private BlockPointer mRootNodeBlockPointer;
//	private HashTable mHashTable;
//
//
//	HashTableRootNode(HashTable aHashTable, boolean aCreate)
//	{
//		mHashTable = aHashTable;
//
//		if (aCreate)
//		{
//			mRootNode = new HashTableLeafNode(mHashTable, null);
//			mRootNodeBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
//		}
//	}
//
//
//	public void loadRoot()
//	{
//		Log.i("load root %s", mRootNodeBlockPointer);
//
//		if (mRootNodeBlockPointer.getBlockType() == BlockType.LEAF)
//		{
//			mRootNode = new HashTableLeafNode(mHashTable, null, mRootNodeBlockPointer);
//		}
//		else
//		{
//			mRootNode = new HashTableInnerNode(mHashTable, mRootNodeBlockPointer);
//		}
//	}
//
//
//	void marshal(ByteArrayBuffer aBuffer)
//	{
//		mRootNodeBlockPointer.marshal(aBuffer);
//	}
//
//
//	void unmarshal(ByteArrayBuffer aBuffer)
//	{
//		mRootNodeBlockPointer = new BlockPointer();
//		mRootNodeBlockPointer.unmarshal(aBuffer);
//	}
//
//
//	Iterator<ArrayMapEntry> iterator()
//	{
//		return new HashTableNodeIterator(mHashTable, mRootNode);
//	}
//
//
//	@Override
//	public BlockPointer getRootNodeBlockPointer()
//	{
//		return mRootNodeBlockPointer;
//	}
//
//
//	void writeBlock()
//	{
//		mHashTable.freeBlock(mRootNodeBlockPointer);
//
//		mRootNodeBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
//	}
//
//
//	void rollback(boolean aWasEmptyInstance)
//	{
//		mRootNode = null;
//
//		if (aWasEmptyInstance)
//		{
//			Log.d("rollback empty");
//
//			// occurs when the hashtable is created and never been commited thus rollback is to an empty hashtable
//			mRootNode = new HashTableLeafNode(mHashTable, null);
//		}
//		else
//		{
//			Log.d("rollback %s", mRootNodeBlockPointer.getBlockType() == BlockType.LEAF ? "root map" : "root node");
//
//			loadRoot();
//		}
//	}
//
//
//	void close()
//	{
//		mRootNode = null;
//	}
//
//
//	@Override
//	public String integrityCheck()
//	{
//		return mRootNode.integrityCheck();
//	}
//
//
//	@Override
//	public void visit(HashTableVisitor aVisitor)
//	{
//		mRootNode.visit(aVisitor);
//	}
//
//
//	@Override
//	public byte[] array()
//	{
//		return mRootNode.array();
//	}
//
//
//	@Override
//	public BlockType getType()
//	{
//		return mRootNode.getType();
//	}
//
//
//	@Override
//	public boolean getValue(ArrayMapEntry aEntry, long aHash, int aLevel)
//	{
//		return mRootNode.getValue(aEntry, aHash, aLevel);
//	}
//
//
//	@Override
//	public boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
//	{
//		if (mRootNode instanceof HashTableLeafNode)
//		{
//			Log.d("put root value");
//
//			if (mRootNode.putValue(aEntry, oOldEntry, aHash, 0))
//			{
//				return true;
//			}
//
//			Log.d("upgrade root from leaf to node");
//
//			mRootNode = ((HashTableLeafNode)mRootNode).splitLeaf(0);
//
//			mRootNodeBlockPointer = mHashTable.writeBlock(mRootNode, mHashTable.mPointersPerNode);
//		}
//
//		return mRootNode.putValue(aEntry, oOldEntry, aHash, 0);
//	}
//
//
//	@Override
//	public boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
//	{
//		return mRootNode.removeValue(aEntry, oOldEntry, aHash, aLevel);
//	}
//
//
//	@Override
//	public void scan(ScanResult aScanResult)
//	{
//		mRootNode.scan(aScanResult);
//	}
}
