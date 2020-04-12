package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableLeafNode extends ArrayMap implements HashTableNode
{
	private HashTable mHashTable;
	private HashTableInnerNode mParent;
	BlockPointer mBlockPointer;


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParent)
	{
		super(aHashTable.mLeafSize);

		mHashTable = aHashTable;
		mParent = aParent;
	}


	public HashTableLeafNode(HashTable aHashTable, HashTableInnerNode aParent, BlockPointer aBlockPointer)
	{
		super(aHashTable.readBlock(aBlockPointer));

		mHashTable = aHashTable;
		mParent = aParent;
		mBlockPointer = aBlockPointer;

		assert mHashTable.mPerformanceTool.tick("readLeaf");

		assert aBlockPointer.getBlockType() == BlockType.LEAF;

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


	HashTableInnerNode splitLeaf(int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mHashTable.freeBlock(mBlockPointer);

		HashTableLeafNode lowLeaf = new HashTableLeafNode(mHashTable, mParent);
		HashTableLeafNode highLeaf = new HashTableLeafNode(mHashTable, mParent);
		int halfRange = mHashTable.mPointersPerNode / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		HashTableInnerNode node = new HashTableInnerNode(mHashTable);
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

		divideLeafEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		aParent.writeBlock(aIndex, lowLeaf, halfRange);
		aParent.writeBlock(aIndex + halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeafNode aLowLeaf, HashTableLeafNode aHighLeaf)
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
			HashTableInnerNode node = splitLeaf(aLevel + 1);

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

		aScanResult.enterLeaf(mBlockPointer, mBuffer);
		aScanResult.records += size();
		aScanResult.exitLeaf();
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		aVisitor.visit(this);
	}
}
