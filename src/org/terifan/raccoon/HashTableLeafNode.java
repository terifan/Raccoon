package org.terifan.raccoon;

import java.io.IOException;
import java.util.Iterator;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableLeafNode implements HashTableNode
{
	final static int OVERHEAD = ArrayMap.OVERHEAD;

	private HashTable mHashTable;
	private HashTableInnerNode mParentNode;
	private BlockPointer mBlockPointer;
	private ArrayMap mMap;


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParentNode)
	{
		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mMap = new ArrayMap(aHashTable.mLeafSize);
	}


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParentNode, BlockPointer aBlockPointer)
	{
		assert aBlockPointer.getBlockType() == BlockType.LEAF;
		assert aHashTable.mPerformanceTool.tick("readLeaf");

		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mBlockPointer = aBlockPointer;

		mMap = new ArrayMap(aHashTable.readBlock(mBlockPointer));

		mHashTable.mCost.mReadBlockLeaf++;
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}


	@Override
	public boolean get(ArrayMapEntry aEntry, long aHash, int aLevel)
	{
		mHashTable.mCost.mValueGet++;
		return mMap.get(aEntry);
	}


	@Override
	public boolean put(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		return mMap.put(aEntry, oOldEntry);
	}


	@Override
	public boolean remove(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		return mMap.remove(aEntry, oOldEntry);
	}


	HashTableInnerNode growTree(int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("growTree");

		Log.inc();
		Log.d("grow tree");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mParentNode.freeNode(this);

		int halfRange = mHashTable.mPointersPerNode / 2;

		HashTableInnerNode parent = new HashTableInnerNode(mHashTable, mParentNode);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, parent);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, parent);

		divideEntries(aLevel, halfRange, lowLeaf, highLeaf);

		parent.setNode(0, lowLeaf, halfRange);
		parent.setNode(halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return parent;
	}


	HashTableInnerNode splitRootLeaf()
	{
		assert mHashTable.mPerformanceTool.tick("splitRootLeaf");

		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mParentNode.freeNode(this);

		int halfRange = mHashTable.mPointersPerNode / 2;

		HashTableInnerNode parent = new HashTableInnerNode(mHashTable, mParentNode);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, parent);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, parent);

		divideEntries(0, halfRange, lowLeaf, highLeaf);

		parent.setNode(0, lowLeaf, halfRange);
		parent.setNode(halfRange, highLeaf, halfRange);

		Log.dec();

		return parent;
	}


	boolean splitLeaf(int aIndex, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		if (mBlockPointer.getUserData() == 1)
		{
			return false;
		}

		assert mBlockPointer.getUserData() >= 2;

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mParentNode.freeNode(this);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, mParentNode);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, mParentNode);
		int halfRange = (int)mBlockPointer.getUserData() / 2;

		divideEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		mParentNode.setNode(aIndex, lowLeaf, halfRange);
		mParentNode.setNode(aIndex + halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideEntries(int aLevel, int aHalfRange, HashTableLeafNode aLowLeaf, HashTableLeafNode aHighLeaf)
	{
		assert mHashTable.mPerformanceTool.tick("divideLeafEntries");

		for (ArrayMapEntry entry : mMap)
		{
			if (mHashTable.computeIndex(mHashTable.computeHash(entry.getKey()), aLevel) < aHalfRange)
			{
				aLowLeaf.mMap.put(entry, null);
			}
			else
			{
				aHighLeaf.mMap.put(entry, null);
			}
		}
	}


	void putValueLeaf(int aIndex, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");

		mHashTable.mCost.mTreeTraversal++;

		if (mMap.put(aEntry, oOldEntry))
		{
			mParentNode.freeNode(this);

			mParentNode.setNode(aIndex, this, (int)mBlockPointer.getUserData());

			mHashTable.mCost.mValuePut++;
		}
		else if (splitLeaf(aIndex, aLevel))
		{
			mParentNode.put(aEntry, oOldEntry, aHash, aLevel); // recursive put
		}
		else
		{
			HashTableInnerNode node = growTree(aLevel + 1);

			node.put(aEntry, oOldEntry, aHash, aLevel + 1); // recursive put

			mParentNode.setNode(aIndex, node, (int)mBlockPointer.getUserData());
		}
	}


	void upgradeHole(int aIndex, ArrayMapEntry aEntry, int aLevel, int aRange)
	{
		assert mHashTable.mPerformanceTool.tick("upgradeHole");

		mHashTable.mCost.mTreeTraversal++;

		mMap.put(aEntry, null);

		mParentNode.setNode(aIndex, this, aRange);

		mHashTable.mCost.mValuePut++;
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		assert mHashTable.mPerformanceTool.tick("scan");

		aScanResult.enterLeafNode(mBlockPointer, mMap.array());
		aScanResult.leafNodes++;

		for (ArrayMapEntry entry : mMap)
		{
			aScanResult.records++;
			aScanResult.record();

			if ((entry.getFlags() & TableInstance.FLAG_BLOB) != 0)
			{
				try
				{
					new Blob(mHashTable.mBlockAccessor, null, entry.getValue(), BlobOpenOption.READ).scan(aScanResult);
				}
				catch (IOException e)
				{
					throw new DatabaseIOException(e);
				}
			}
		}

		aScanResult.exitLeafNode();
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		aVisitor.visit(this);
	}


	@Override
	public BlockPointer flush()
	{
		mParentNode.freeNode(this);

		mBlockPointer = mHashTable.writeBlock(this, mHashTable.mPointersPerNode);

		return mBlockPointer;
	}


	@Override
	public void clear()
	{
		for (ArrayMapEntry entry : mMap)
		{
			if ((entry.getFlags() & TableInstance.FLAG_BLOB) != 0)
			{
				Blob.deleteBlob(mHashTable.mBlockAccessor, entry.getValue());
			}
		}

		mParentNode.freeNode(this);
	}


	@Override
	public String integrityCheck()
	{
		return mMap.integrityCheck();
	}


	@Override
	public byte[] array()
	{
		return mMap.array();
	}


	public int size()
	{
		return mMap.size();
	}


	public Iterator<ArrayMapEntry> iterator()
	{
		return mMap.iterator();
	}
}