package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;



class HashTableLeaf extends ArrayMap implements Node
{
	private HashTable mHashTable;
	BlockPointer mBlockPointer;


	public HashTableLeaf(HashTable aHashTable)
	{
		super(aHashTable.mLeafSize);

		mHashTable = aHashTable;
	}


	public HashTableLeaf(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		super(aHashTable.readBlock(aBlockPointer));

		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;

		assert mHashTable.mPerformanceTool.tick("readLeaf");

		assert aBlockPointer.getBlockType() == BlockType.LEAF;

		mHashTable.mCost.mReadBlockLeaf++;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}


	HashTableNode splitLeaf(BlockPointer aBlockPointer, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");
		assert mBlockPointer == aBlockPointer;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;
		mHashTable.mCost.mBlockSplit++;

		mHashTable.freeBlock(aBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable);
		int halfRange = mHashTable.mPointersPerNode / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = mHashTable.writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = mHashTable.writeIfNotEmpty(highLeaf, halfRange);

		HashTableNode node = new HashTableNode(mHashTable);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(BlockPointer aBlockPointer, int aIndex, int aLevel, HashTableNode aNode)
	{
		assert mHashTable.mPerformanceTool.tick("splitLeaf");
		assert mBlockPointer == aBlockPointer;

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

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable);
		int halfRange = aBlockPointer.getRange() / 2;

		divideLeafEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = mHashTable.writeIfNotEmpty(lowLeaf, halfRange);
		BlockPointer highIndex = mHashTable.writeIfNotEmpty(highLeaf, halfRange);

		aNode.split(aIndex, lowIndex, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return true;
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


	byte[] putValueLeaf(HashTableNode aNode, BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel, byte[] aKey)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");
		assert mBlockPointer == aBlockPointer;

		mHashTable.mCost.mTreeTraversal++;

		byte[] oldValue;

		if (put(aEntry))
		{
			oldValue = aEntry.getValue();

			mHashTable.freeBlock(aBlockPointer);

			mBlockPointer = mHashTable.writeBlock(this, aBlockPointer.getRange());

			aNode.setPointer(aIndex, mBlockPointer);

			mHashTable.mCost.mValuePut++;
		}
		else if (splitLeaf(aBlockPointer, aIndex, aLevel, aNode))
		{
			oldValue = aNode.putValue(aEntry, aKey, aLevel); // recursive put
		}
		else
		{
			HashTableNode node = splitLeaf(aBlockPointer, aLevel + 1);

			oldValue = node.putValue(aEntry, aKey, aLevel + 1); // recursive put

			mBlockPointer = mHashTable.writeBlock(node, aBlockPointer.getRange());

			aNode.setPointer(aIndex, mBlockPointer);

			node.gc();
		}

		return oldValue;
	}
}
