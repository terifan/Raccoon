package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;



class HashTableLeaf extends ArrayMap implements Node
{
	private HashTable mHashTable;


	public HashTableLeaf(HashTable aHashTable, int aCapacity)
	{
		super(aCapacity);

		mHashTable = aHashTable;
	}


	public HashTableLeaf(HashTable aHashTable, byte[] aBuffer)
	{
		super(aBuffer);

		mHashTable = aHashTable;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}


	HashTableNode splitLeaf(BlockPointer aBlockPointer, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mHashTable.freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, mHashTable.mLeafSize);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, mHashTable.mLeafSize);
		int halfRange = mHashTable.mPointersPerNode / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = mHashTable.writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = mHashTable.writeIfNotEmpty(highLeaf, halfRange);

		HashTableNode node = new HashTableNode(mHashTable, new byte[mHashTable.mNodeSize]);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return node;
	}


	private void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		assert mHashTable.mPerformanceTool.tick("divideLeafEntries");

		for (ArrayMapEntry entry : this)
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
}
