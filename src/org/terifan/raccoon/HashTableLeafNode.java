package org.terifan.raccoon;

import java.io.IOException;
import org.terifan.raccoon.io.DatabaseIOException;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableLeafNode extends ArrayMap implements HashTableNode
{
	private HashTable mHashTable;
	private HashTableInnerNode mParentNode;
	private BlockPointer mBlockPointer;


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParent)
	{
		super(aHashTable.mLeafSize);

		mHashTable = aHashTable;
		mParentNode = aParent;
	}


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParentNode, BlockPointer aBlockPointer)
	{
		super(aHashTable.readBlock(aBlockPointer));

		assert aBlockPointer.getBlockType() == BlockType.LEAF;
		assert aHashTable.mPerformanceTool.tick("readLeaf");

		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mBlockPointer = aBlockPointer;

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
	public boolean getValue(ArrayMapEntry aEntry, long aHash, int aLevel)
	{
		mHashTable.mCost.mValueGet++;
		return get(aEntry);
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		return put(aEntry, oOldEntry);
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		return remove(aEntry, oOldEntry);
	}


	HashTableInnerNode growTree(int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("growTree");

		Log.inc();
		Log.d("grow tree");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		if (mBlockPointer != null)
		{
			mHashTable.freeBlock(mBlockPointer);
		}

		int halfRange = mHashTable.mPointersPerNode / 2;

		HashTableInnerNode node = new HashTableInnerNode(mHashTable, mParentNode);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, node);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, node);

		divideEntries(aLevel, halfRange, lowLeaf, highLeaf);

		node.writeBlock(0, lowLeaf, halfRange);
		node.writeBlock(halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(int aIndex, int aLevel, HashTableInnerNode aParent)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		if (mBlockPointer.getRange() == 1)
		{
			return false;
		}

		assert mBlockPointer.getRange() >= 2;

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.freeBlock(mBlockPointer);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, aParent);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, aParent);
		int halfRange = mBlockPointer.getRange() / 2;

		divideEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		aParent.writeBlock(aIndex, lowLeaf, halfRange);
		aParent.writeBlock(aIndex + halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideEntries(int aLevel, int aHalfRange, HashTableLeafNode aLowLeaf, HashTableLeafNode aHighLeaf)
	{
		assert mHashTable.mPerformanceTool.tick("divideLeafEntries");

		for (ArrayMapEntry entry : this)
		{
			if (mHashTable.computeIndex(mHashTable.computeHash(entry.getKey()), aLevel) < aHalfRange)
			{
				aLowLeaf.put(entry, null);
			}
			else
			{
				aHighLeaf.put(entry, null);
			}
		}
	}


	void putValueLeaf(HashTableInnerNode aParent, int aIndex, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");

		mHashTable.mCost.mTreeTraversal++;

		if (put(aEntry, oOldEntry))
		{
			if (mBlockPointer.getBlockType() != BlockType.HOLE)
			{
				mHashTable.freeBlock(mBlockPointer);
			}

			aParent.writeBlock(aIndex, this, mBlockPointer.getRange());

			mHashTable.mCost.mValuePut++;
		}
		else if (splitLeaf(aIndex, aLevel, aParent))
		{
			aParent.putValue(aEntry, oOldEntry, aHash, aLevel); // recursive put
		}
		else
		{
			HashTableInnerNode node = growTree(aLevel + 1);

			node.putValue(aEntry, oOldEntry, aHash, aLevel + 1); // recursive put

			aParent.writeBlock(aIndex, node, mBlockPointer.getRange());
		}
	}


	void upgradeHole(HashTableInnerNode aParent, int aIndex, ArrayMapEntry aEntry, int aLevel, int aRange)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");

		mHashTable.mCost.mTreeTraversal++;

		put(aEntry, null);

		aParent.writeBlock(aIndex, this, aRange);

		mHashTable.mCost.mValuePut++;
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		assert mHashTable.mPerformanceTool.tick("scan");

		aScanResult.enterLeafNode(mBlockPointer, mBuffer);
		aScanResult.leafNodes++;

		for (ArrayMapEntry entry : this)
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
		if (mBlockPointer != null)
		{
			mHashTable.freeBlock(mBlockPointer);
		}

		mBlockPointer = mHashTable.writeBlock(this, mHashTable.mPointersPerNode);

		return mBlockPointer;
	}
}
