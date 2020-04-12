package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTableLeaf extends ArrayMap implements Node
{
	private HashTable mHashTable;
	private HashTableNode mParent;
	BlockPointer mBlockPointer;


	public HashTableLeaf(HashTable aHashTable, HashTableNode aParent)
	{
		super(aHashTable.mLeafSize);

		mHashTable = aHashTable;
		mParent = aParent;
	}


	public HashTableLeaf(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
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


	HashTableNode splitLeaf(int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mHashTable.freeBlock(mBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, mParent);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, mParent);
		int halfRange = mHashTable.mPointersPerNode / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		HashTableNode node = new HashTableNode(mHashTable);
		node.writeBlock(0, lowLeaf, halfRange);
		node.writeBlock(halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(int aIndex, int aLevel, HashTableNode aParent)
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

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, aParent);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, aParent);
		int halfRange = mBlockPointer.getRange() / 2;

		divideLeafEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		aParent.writeBlock(aIndex, lowLeaf, halfRange);
		aParent.writeBlock(aIndex + halfRange, highLeaf, halfRange);

		Log.dec();
		Log.dec();

		return true;
	}


	private void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
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


	void putValueLeaf(HashTableNode aParent, int aIndex, ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
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
			HashTableNode node = splitLeaf(aLevel + 1);

			node.putValue(aEntry, oOldEntry, aHash, aLevel + 1); // recursive put

			aParent.writeBlock(aIndex, node, mBlockPointer.getRange());
		}
	}


	void upgradeHole(HashTableNode aParent, int aIndex, ArrayMapEntry aEntry, int aLevel, int aRange)
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
