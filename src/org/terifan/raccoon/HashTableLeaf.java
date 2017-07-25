package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;
import org.terifan.security.messagedigest.MurmurHash3;


class HashTableLeaf extends Node
{
	private ArrayMap mMap;
	private HashTable mHashTable;
	private BlockPointer mBlockPointer;


	public HashTableLeaf(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
		mMap = new ArrayMap(mHashTable.getBlockAccessor().readBlock(mBlockPointer));
	}


	public HashTableLeaf(HashTable aHashTable)
	{
		mHashTable = aHashTable;
		mMap = new ArrayMap(mHashTable.getLeafSize());
	}


	@Override
	public byte[] array()
	{
		return mMap.array();
	}


	@Override
	public BlockType getType()
	{
		return BlockType.LEAF;
	}


	HashTableNode splitLeaf(int aLevel)
	{
		Log.inc();
		Log.d("split leaf");
		Log.inc();

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable);
		int halfRange = mHashTable.getPointersPerNode() / 2;

		divideLeafEntries(aLevel, halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(halfRange);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(halfRange);

		HashTableNode node = new HashTableNode(mHashTable);
		node.setPointer(0, lowIndex);
		node.setPointer(halfRange, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return node;
	}


	boolean splitLeaf(int aIndex, int aLevel, HashTableNode aParent)
	{
		if (mBlockPointer.getRange() == 1)
		{
			return false;
		}

		assert mBlockPointer.getRange() >= 2;

		Log.inc();
		Log.d("split leaf");
		Log.inc();

		freeBlock();

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable);
		int halfRange = mBlockPointer.getRange() / 2;

		divideLeafEntries(aLevel, aIndex + halfRange, lowLeaf, highLeaf);

		// create nodes pointing to leafs
		BlockPointer lowIndex = lowLeaf.writeIfNotEmpty(halfRange);
		BlockPointer highIndex = highLeaf.writeIfNotEmpty(halfRange);

		aParent.split(aIndex, lowIndex, highIndex);

		lowLeaf.gc();
		highLeaf.gc();

		Log.dec();
		Log.dec();

		return true;
	}


	void divideLeafEntries(int aLevel, int aHalfRange, HashTableLeaf aLowLeaf, HashTableLeaf aHighLeaf)
	{
		for (ArrayMapEntry entry : mMap)
		{
			if (computeIndex(entry.getKey(), aLevel) < aHalfRange)
			{
				aLowLeaf.mMap.put(entry);
			}
			else
			{
				aHighLeaf.mMap.put(entry);
			}
		}
	}


	BlockPointer writeIfNotEmpty(int aRange)
	{
		if (mMap.isEmpty())
		{
			mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}
		else
		{
			mBlockPointer = writeBlock(aRange);
		}

		return mBlockPointer;
	}


	void freeBlock()
	{
		mHashTable.getBlockAccessor().freeBlock(mBlockPointer);
	}


	BlockPointer writeBlock(int aRange)
	{
		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(mMap.array(), 0, mMap.array().length, mHashTable.getTransactionId().get(), getType(), aRange);

		return mBlockPointer;
	}


	int computeIndex(byte[] aKey, int aLevel)
	{
		return MurmurHash3.hash32(aKey, mHashTable.getHashSeed() ^ aLevel) & (mHashTable.getPointersPerNode() - 1);
	}


	void gc()
	{
		mMap.gc();
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


	boolean remove(ArrayMapEntry aEntry)
	{
		return mMap.remove(aEntry);
	}


	void clear()
	{
		mMap.clear();
	}


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
}
