package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class HashTableRoot
{
	HashTableLeaf mRootMap;
	private HashTableNode mRootNode;
	BlockPointer mRootBlockPointer;
	private HashTable mHashTable;
	private int mLeafSize;
	private int mPointersPerNode;
	private int mNodeSize;


	HashTableRoot(HashTable aHashTable)
	{
		mHashTable = aHashTable;
	}


	HashTableRoot(HashTable aHashTable, int aNodeSize, int aLeafSize, int aPointersPerNode)
	{
		mHashTable = aHashTable;
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mPointersPerNode = aPointersPerNode;
		mRootMap = new HashTableLeaf(mHashTable);
		mRootBlockPointer = mHashTable.writeBlock(mRootMap, aPointersPerNode);
	}


	public void loadRoot()
	{
		Log.i("load root %s", mRootBlockPointer);

		if (mRootBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRootMap = new HashTableLeaf(mHashTable, mRootBlockPointer);
		}
		else
		{
			mRootNode = new HashTableNode(mHashTable, mRootBlockPointer);
		}
	}


	void init(int aNodeSize, int aLeafSize, int aPointersPerNode)
	{
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mPointersPerNode = aPointersPerNode;
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
			return mRootMap.get(aEntry);
		}

		return mRootNode.getValue(aEntry.getKey(), 0, aEntry);
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

				mRootBlockPointer = mHashTable.writeBlock(mRootNode, mPointersPerNode);
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
			modified = mRootNode.removeValue(aEntry.getKey(), 0, aEntry);
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
			mRootBlockPointer = mHashTable.writeBlock(mRootMap, mPointersPerNode);
		}
		else
		{
			mRootBlockPointer = mHashTable.writeBlock(mRootNode, mPointersPerNode);
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
			mRootMap = new HashTableLeaf(mHashTable);
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
			mRootMap = new HashTableLeaf(mHashTable);
		}

		mHashTable.freeBlock(mRootBlockPointer);

		mRootBlockPointer = mHashTable.writeBlock(mRootMap, mPointersPerNode);
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


	void scan(ScanResult aScanResult)
	{
		scan(aScanResult, mRootBlockPointer);
	}


	void scan(ScanResult aScanResult, BlockPointer aBlockPointer)
	{
		assert mHashTable.mPerformanceTool.tick("scan");

		switch (aBlockPointer.getBlockType())
		{
			case INDEX:
			{
				aScanResult.enterNode(aBlockPointer);
				aScanResult.indexBlocks++;

				HashTableNode indexNode = new HashTableNode(mHashTable, aBlockPointer);

				for (int i = 0; i < indexNode.getPointerCount(); i++)
				{
					BlockPointer pointer = indexNode.getPointer(i);

					if (pointer != null)
					{
						if (pointer.getBlockType() == BlockType.HOLE)
						{
							aScanResult.holes++;
						}
						else
						{
							scan(aScanResult, pointer);
						}
					}
				}
				aScanResult.exitNode();
				break;
			}
			case LEAF:
			{
				HashTableLeaf leafNode = new HashTableLeaf(mHashTable, aBlockPointer);

				aScanResult.enterLeaf(aBlockPointer, leafNode.array());

				aScanResult.records += leafNode.size();

//				for (RecordEntry entry : leafNode)
//				{
//					aScanResult.entry();
//
//					if (entry.hasFlag(LeafEntry.FLAG_BLOB))
//					{
//						aScanResult.blobs++;
//
//						ByteArrayBuffer byteArrayBuffer = ByteArrayBuffer.wrap(entry.getValue());
//						byteArrayBuffer.readInt8();
//						long len = byteArrayBuffer.readVar64();
//
//						while (byteArrayBuffer.remaining() > 0)
//						{
//							scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));
//						}
//					}
//					else
//					{
//						aScanResult.records++;
//					}
//				}
				aScanResult.exitLeaf();

				break;
			}
			case BLOB_INDEX:
			{
				aScanResult.blobIndices++;

				byte[] buffer = mHashTable.mBlockAccessor.readBlock(aBlockPointer);

				ByteArrayBuffer byteArrayBuffer = ByteArrayBuffer.wrap(buffer);
				while (byteArrayBuffer.remaining() > 0)
				{
					aScanResult.enterBlob();

					scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));

					aScanResult.exitBlob();
				}
				break;
			}
			case BLOB_DATA:
			{
				aScanResult.blobData++;

				aScanResult.blobData();

				break;
			}
			default:
				throw new IllegalStateException();
		}
	}
}
