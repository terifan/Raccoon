package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.Log;


class HashTableLeaf extends Node
{
	private ArrayMap mMap;
	HashMap<Integer, Node> mChildren = new HashMap<>();


	public HashTableLeaf(HashTable aHashTable, HashTableNode aParent, byte[] aBuffer)
	{
		super(aHashTable, aParent, null);

		mMap = new ArrayMap(aBuffer);
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
		assert mBlockPointer.getLevel() == 0 || mBlockPointer.getRangeSize() == 1;

		Log.inc();
		Log.d("grow leaf");
		Log.inc();

		freeBlock();

		HashTableNode node = new HashTableNode(mHashTable, mParent, new byte[mHashTable.getNodeSize()]);
		node.setBlockPointer(new BlockPointer().setLevel(mBlockPointer.getLevel()).setRangeOffset(mBlockPointer.getRangeOffset()).setRangeSize(mBlockPointer.getRangeSize()));

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, node, new byte[mHashTable.getLeafSize()]);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, node, new byte[mHashTable.getLeafSize()]);
		int halfRange = mHashTable.getPointersPerNode() / 2;
		int level = mBlockPointer.getLevel();

		divideLeafEntries(level, halfRange, lowLeaf, highLeaf);

		node.setPointer(lowLeaf.updatePointer(0, halfRange, level + 1));
		node.setPointer(highLeaf.updatePointer(halfRange, halfRange, level + 1));

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

		HashTableLeaf lowLeaf = new HashTableLeaf(mHashTable, mParent, new byte[mHashTable.getLeafSize()]);
		HashTableLeaf highLeaf = new HashTableLeaf(mHashTable, mParent, new byte[mHashTable.getLeafSize()]);

		int rangeOffset = mBlockPointer.getRangeOffset();
		int halfRange = mBlockPointer.getRangeSize() / 2;
		int level = mBlockPointer.getLevel();

		divideLeafEntries(level - 1, rangeOffset + halfRange, lowLeaf, highLeaf);

		mParent.set(rangeOffset, lowLeaf.updatePointer(rangeOffset, halfRange, level));
		mParent.set(rangeOffset + halfRange, highLeaf.updatePointer(rangeOffset + halfRange, halfRange, level));

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


	private BlockPointer updatePointer(int aRangeOffset, int aRangeSize, int aLevel)
	{
		if (mMap.isEmpty())
		{
			return mBlockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRangeOffset(aRangeOffset).setRangeSize(aRangeSize).setLevel(aLevel);
		}

		return mBlockPointer = new BlockPointer().setBlockType(BlockType.LEAF).setRangeOffset(aRangeOffset).setRangeSize(aRangeSize).setLevel(aLevel);
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


	@Override
	void freeBlock()
	{
		if (mBlockPointer.getBlockType() != BlockType.FREE && mBlockPointer.getBlockType() != BlockType.PENDING_WRITE)
		{
			System.out.println("free    " + mBlockPointer);

			mHashTable.getBlockAccessor().freeBlock(mBlockPointer);

			mBlockPointer.setBlockType(BlockType.FREE);

			mChildren.remove(mBlockPointer.getRangeOffset());
		}
	}


	@Override
	void flush()
	{
		for (Node node : mChildren.values())
		{
			node.flush();
		}

		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId(), getBlockType(), mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());

		System.out.println("flush   " + mBlockPointer);

		if (mParent != null)
		{
			mParent.setPointer(mBlockPointer);
		}
	}


	@Override
	void writeBlock()
	{
		writeBlock(mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
	}


	@Override
	void writeBlock(int aRangeOffset, int aRangeSize, int aLevel)
	{
		assert aLevel != 0 || aRangeOffset == 0 && aRangeSize * BlockPointer.SIZE == mHashTable.getNodeSize();

		mBlockPointer = new BlockPointer();
		mBlockPointer.setBlockType(BlockType.PENDING_WRITE);
		mBlockPointer.setRangeOffset(aRangeOffset);
		mBlockPointer.setRangeSize(aRangeSize);
		mBlockPointer.setLevel(aLevel);

		if (mParent != null)
		{
			mParent.setPointer(mBlockPointer);
		}

		System.out.println("reserve {level=" + aLevel + ", range=" + aRangeOffset + ":" + aRangeSize + "}");
	}
}
