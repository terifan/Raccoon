package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


class HashTableLeaf extends Node
{
	private ArrayMap mMap;


	public HashTableLeaf(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		super(aHashTable, aParent, aBlockPointer);

		mMap = new ArrayMap(mHashTable.getBlockAccessor().readBlock(mBlockPointer));
	}


	public HashTableLeaf(HashTable aHashTable, HashTableNode aParent)
	{
		super(aHashTable, aParent, null);

		mMap = new ArrayMap(mHashTable.getLeafSize());
	}


	@Override
	public byte[] array()
	{
		return mMap.array();
	}


	@Override
	public BlockType getBlockType()
	{
		return BlockType.LEAF;
	}


	HashTableNode expandLeaf(int aLevel)
	{
		assert aLevel == 0 || mBlockPointer.getRangeSize() == 1;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		HashTableNode node = new HashTableNode(mHashTable, mParent);

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, node);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, node);
		int halfRange = mHashTable.getPointersPerNode() / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(0, halfRange, aLevel + 1);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(halfRange, halfRange, aLevel + 1);

		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(int aIndex, int aLevel, HashTableNode aParent)
	{
		assert mBlockPointer.getRangeSize() > 1;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		freeBlock();

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, aParent);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, aParent);
		int halfRange = mBlockPointer.getRangeSize() / 2;

		divideLeafEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(aIndex, halfRange, aLevel);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(aIndex + halfRange, halfRange, aLevel);

		aParent.split(aIndex, lowIndex, highIndex);

		Log.dec();
		Log.dec();

		return true;
	}


	void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		for (ArrayMapEntry entry : mMap)
		{
			if (mHashTable.computeIndex(entry.getKey(), aLevel) < aHalfRange)
			{
				aLowLeaf.mMap.put(entry);
			}
			else
			{
				aHighLeaf.mMap.put(entry);
			}
		}
	}


	BlockPointer writeIfNotEmpty(int aRangeOffset, int aRangeSize, int aLevel)
	{
		if (mMap.isEmpty())
		{
			mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRangeOffset(aRangeOffset).setRangeSize(aRangeSize);
		}
		else
		{
			mBlockPointer = writeBlock(aRangeOffset, aRangeSize, aLevel);
		}

		return mBlockPointer;
	}


	Integer size()
	{
		return mMap.size();
	}


	boolean isEmpty()
	{
		return mMap.isEmpty();
	}


	@Override
	boolean get(ArrayMapEntry aEntry, int aLevel)
	{
		return mMap.get(aEntry);
	}


	@Override
	boolean put(ArrayMapEntry aEntry, int aLevel)
	{
		return mMap.put(aEntry);
	}


	@Override
	boolean remove(ArrayMapEntry aEntry, int aLevel)
	{
		return mMap.remove(aEntry);
	}


	void clear()
	{
		mMap.clear();
	}


	@Override
	String integrityCheck()
	{
		return mMap.integrityCheck();
	}


	Iterator<ArrayMapEntry> iterator()
	{
		return mMap.iterator();
	}


	static int getOverhead()
	{
		return ArrayMap.OVERHEAD;
	}


	public ArrayMap getMap()
	{
		return mMap;
	}
}
