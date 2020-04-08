package org.terifan.raccoon;

import java.util.ArrayList;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


public class HashTableRoot
{
	private HashTableLeaf mRootMap;
	private HashTableNode mRootNode;
	private BlockPointer mRootBlockPointer;
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
		mRootMap = new HashTableLeaf(aLeafSize);
		mRootBlockPointer = mHashTable.writeBlock(mRootMap, aPointersPerNode);
	}


	public void loadRoot()
	{
		Log.i("load root %s", mRootBlockPointer);

		if (mRootBlockPointer.getBlockType() == BlockType.LEAF)
		{
			mRootMap = mHashTable.readLeaf(mRootBlockPointer);
		}
		else
		{
			mRootNode = mHashTable.readNode(mRootBlockPointer).setGCEnabled(false);
		}
	}


	void init(int aNodeSize, int aLeafSize, int aPointersPerNode)
	{
		mNodeSize = aNodeSize;
		mLeafSize = aLeafSize;
		mPointersPerNode = aPointersPerNode;
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		if (mRootMap != null)
		{
			return mRootMap.get(aEntry);
		}

		return getValue(aEntry.getKey(), 0, aEntry, mRootNode);
	}


	boolean getValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry, HashTableNode aNode)
	{
		assert mHashTable.mPerformanceTool.tick("getValue");

		Log.i("get %s value", mHashTable.mTableName);

		mHashTable.mCost.mTreeTraversal++;

		BlockPointer blockPointer = aNode.getPointer(aNode.findPointer(mHashTable.computeIndex(aKey, aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return getValue(aKey, aLevel + 1, aEntry, readNode(blockPointer));
			case LEAF:
				mHashTable.mCost.mValueGet++;
				HashTableLeaf leaf = readLeaf(blockPointer);
				boolean result = leaf.get(aEntry);
				leaf.gc();
				return result;
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
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

				mRootNode = splitLeaf(mRootBlockPointer, mRootMap, 0).setGCEnabled(false);

				mRootBlockPointer = mHashTable.writeBlock(mRootNode, mPointersPerNode);
				mRootMap = null;
			}
		}

		if (mRootMap == null)
		{
			putValue(aEntry, aEntry.getKey(), 0, mRootNode);
		}
	}


	byte[] putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel, HashTableNode aNode)
	{
		assert mHashTable.mPerformanceTool.tick("putValue");

		Log.d("put %s value", mHashTable.mTableName);
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		int index = aNode.findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = readNode(blockPointer);
				oldValue = putValue(aEntry, aKey, aLevel + 1, node);
				mHashTable.freeBlock(blockPointer);
				aNode.setPointer(index, mHashTable.writeBlock(node, blockPointer.getRange()));
				node.gc();
				break;
			case LEAF:
				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aNode, aKey);
				break;
			case HOLE:
				oldValue = upgradeHoleToLeaf(aEntry, aNode, blockPointer, index);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel, HashTableNode aNode, byte[] aKey)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");

		mHashTable.mCost.mTreeTraversal++;

		HashTableLeaf map = readLeaf(aBlockPointer);

		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.getValue();

			mHashTable.freeBlock(aBlockPointer);

			aNode.setPointer(aIndex, mHashTable.writeBlock(map, aBlockPointer.getRange()));

			mHashTable.mCost.mValuePut++;
		}
		else if (splitLeaf(map, aBlockPointer, aIndex, aLevel, aNode))
		{
			oldValue = putValue(aEntry, aKey, aLevel, aNode); // recursive put
		}
		else
		{
			HashTableNode node = splitLeaf(aBlockPointer, map, aLevel + 1);

			oldValue = putValue(aEntry, aKey, aLevel + 1, node); // recursive put

			aNode.setPointer(aIndex, mHashTable.writeBlock(node, aBlockPointer.getRange()));

			node.gc();
		}

		return oldValue;
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
			modified = removeValue(aEntry.getKey(), 0, aEntry, mRootNode);
		}
		return modified;
	}


	boolean removeValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry, HashTableNode aNode)
	{
		assert mHashTable.mPerformanceTool.tick("removeValue");

		mHashTable.mCost.mTreeTraversal++;

		int index = aNode.findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = aNode.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			{
				HashTableNode node = readNode(blockPointer);
				if (removeValue(aKey, aLevel + 1, aEntry, node))
				{
					mHashTable.freeBlock(blockPointer);
					BlockPointer newBlockPointer = mHashTable.writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			}
			case LEAF:
			{
				HashTableLeaf node = readLeaf(blockPointer);
				boolean found = node.remove(aEntry);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;

					mHashTable.freeBlock(blockPointer);
					BlockPointer newBlockPointer = mHashTable.writeBlock(node, blockPointer.getRange());
					aNode.setPointer(index, newBlockPointer);
				}

				node.gc();
				return found;
			}
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	byte[] upgradeHoleToLeaf(ArrayMapEntry aEntry, HashTableNode aNode, BlockPointer aBlockPointer, int aIndex)
	{
		assert mHashTable.mPerformanceTool.tick("upgradeHoleToLeaf");

		Log.d("upgrade hole to leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		HashTableLeaf node = new HashTableLeaf(mLeafSize);

		if (!node.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		BlockPointer blockPointer = mHashTable.writeBlock(node, aBlockPointer.getRange());
		aNode.setPointer(aIndex, blockPointer);

		node.gc();

		Log.dec();

		return oldValue;
	}


	HashTableNode splitLeaf(BlockPointer aBlockPointer, HashTableLeaf aLeafNode, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mHashTable.freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mLeafSize);
		HashTableLeaf highLeaf = new HashTableLeaf(mLeafSize);
		int halfRange = mPointersPerNode / 2;

		divideLeafEntries(aLeafNode, aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		HashTableNode node = new HashTableNode(new byte[mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(HashTableLeaf aMap, BlockPointer aBlockPointer, int aIndex, int aLevel, HashTableNode aNode)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		if (aBlockPointer.getRange() == 1)
		{
			return false;
		}

		assert aBlockPointer.getRange() >= 2;

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mLeafSize);
		HashTableLeaf highLeaf = new HashTableLeaf(mLeafSize);
		int halfRange = aBlockPointer.getRange() / 2;

		divideLeafEntries(aMap, aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = writeIfNotEmpty(highLeaf, halfRange);

		aNode.split(aIndex, lowIndex, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideLeafEntries(HashTableLeaf aMap, int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		assert mHashTable.mPerformanceTool.tick("divideLeafEntries");

		for (ArrayMapEntry entry : aMap)
		{
			if (mHashTable.computeIndex(entry.getKey(), aLevel) < aHalfRange)
			{
				aLowLeaf.put(entry);
			}
			else
			{
				aHighLeaf.put(entry);
			}
		}
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
			mRootMap = new HashTableLeaf(mLeafSize);
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
			mRootMap = new HashTableLeaf(mLeafSize);
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
			visitNode(aVisitor, mRootBlockPointer);
		}

		aVisitor.visit(-1, mRootBlockPointer); // start visit at root level
	}


	void visitNode(HashTableVisitor aVisitor, BlockPointer aBlockPointer)
	{
		HashTableNode node = readNode(aBlockPointer);

		for (int i = 0; i < mPointersPerNode; i++)
		{
			BlockPointer next = node.getPointer(i);

			if (next != null && next.getBlockType() == BlockType.INDEX)
			{
				visitNode(aVisitor, next);
			}

			aVisitor.visit(i, next);
		}

		node.gc();
	}


	HashTableLeaf readLeaf(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0() && mRootMap != null)
		{
			return mRootMap;
		}

		return new HashTableLeaf(mHashTable.readBlock(aBlockPointer));
	}


	HashTableNode readNode(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockIndex0() == mRootBlockPointer.getBlockIndex0() && mRootNode != null)
		{
			return mRootNode;
		}

		return new HashTableNode(mHashTable.readBlock(aBlockPointer));
	}


	BlockPointer writeIfNotEmpty(HashTableLeaf aLeaf, int aRange)
	{
		if (aLeaf.isEmpty())
		{
			return new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}

		return mHashTable.writeBlock(aLeaf, aRange);
	}


	void scan(ScanResult aScanResult)
	{
		scan(aScanResult, mRootBlockPointer);
	}


	void scan(ScanResult aScanResult, BlockPointer aBlockPointer)
	{
		assert mHashTable.mPerformanceTool.tick("scan");

		byte[] buffer = mHashTable.mBlockAccessor.readBlock(aBlockPointer);

		switch (aBlockPointer.getBlockType())
		{
			case INDEX:
				aScanResult.enterNode(aBlockPointer);
				aScanResult.indexBlocks++;

				HashTableNode indexNode = new HashTableNode(buffer);

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
			case LEAF:
				HashTableLeaf leafNode = new HashTableLeaf(buffer);

				aScanResult.enterLeaf(aBlockPointer, buffer);

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
			case BLOB_INDEX:
				aScanResult.blobIndices++;

				ByteArrayBuffer byteArrayBuffer = ByteArrayBuffer.wrap(buffer);
				while (byteArrayBuffer.remaining() > 0)
				{
					aScanResult.enterBlob();

					scan(aScanResult, new BlockPointer().unmarshal(byteArrayBuffer));

					aScanResult.exitBlob();
				}
				break;
			case BLOB_DATA:
				aScanResult.blobData++;

				aScanResult.blobData();

				break;
			default:
				throw new IllegalStateException();
		}
	}
}
