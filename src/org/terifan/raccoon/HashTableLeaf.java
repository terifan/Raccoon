package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;


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


	Node split()
	{
		if (mBlockPointer.getLevel() > 0 && mBlockPointer.getRangeSize() > 1)
		{
			splitImpl();
			return mParent;
		}

		return growImpl();
	}


	private HashTableNode growImpl()
	{
		int level = mBlockPointer.getLevel();

		assert level == 0 || mBlockPointer.getRangeSize() == 1;

		Log.inc();
		Log.d("grow leaf");
		Log.inc();

		freeBlock();
		
		HashTableNode node = new HashTableNode(mHashTable, mParent);
		node.setBlockPointer(mBlockPointer);

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, node);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, node);
		int rangeSize = mHashTable.getPointersPerNode() / 2;

		divideLeafEntries(level, rangeSize, lowLeaf, highLeaf);

		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(0, rangeSize, level + 1);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(rangeSize, rangeSize, level + 1);

		node.setPointer(lowIndex);
		node.setPointer(highIndex);

		node.writeBlock();

		Log.dec();
		Log.dec();

		return node;
	}


	private void splitImpl()
	{
		assert mBlockPointer.getRangeSize() > 1;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		freeBlock();

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, mParent);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, mParent);

		int rangeOffset = mBlockPointer.getRangeOffset();
		int rangeSize = mBlockPointer.getRangeSize() / 2;
		int level = mBlockPointer.getLevel();

		divideLeafEntries(level - 1, rangeOffset + rangeSize, lowLeaf, highLeaf);

		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(rangeOffset, rangeSize, level);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(rangeOffset + rangeSize, rangeSize, level);

		mParent.split(rangeOffset, lowIndex, highIndex);

		Log.dec();
		Log.dec();
	}


	void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		for (ArrayMapEntry entry : mMap)
		{
			if (mHashTable.computeIndex(mHashTable.computeHashCode(entry.getKey()), aLevel) < aHalfRange)
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
			mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRangeOffset(aRangeOffset).setRangeSize(aRangeSize).setLevel(aLevel);
		}
		else
		{
			mBlockPointer = writeBlock(aRangeOffset, aRangeSize, aLevel);
		}
		
		assert mBlockPointer.getLevel() != 0 || mBlockPointer.getRangeOffset() == 0 && mBlockPointer.getRangeSize() == mHashTable.getNodeSize() / BlockPointer.SIZE : "Illegal pointer: " + mBlockPointer;

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


	boolean get(ArrayMapEntry aEntry)
	{
		return mMap.get(aEntry);
	}


	boolean put(ArrayMapEntry aEntry)
	{
		return mMap.put(aEntry);
	}


	@Override
	boolean remove(ArrayMapEntry aEntry)
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
