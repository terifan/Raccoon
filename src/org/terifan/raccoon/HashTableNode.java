package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


final class HashTableNode implements Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private byte[] mBuffer;
	private int mPointerCount;
	private boolean mGCEnabled;
	private HashTable mHashTable;


	public HashTableNode(HashTable aHashTable, byte[] aBuffer)
	{
		mHashTable = aHashTable;
		mPointerCount = aBuffer.length / BlockPointer.SIZE;
		mBuffer = aBuffer;
		mGCEnabled = true;
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	@Override
	public BlockType getType()
	{
		return BlockType.INDEX;
	}


	int getPointerCount()
	{
		return mPointerCount;
	}


	void setPointer(int aIndex, BlockPointer aBlockPointer)
	{
		assert get(aIndex).getBlockType() == BlockType.FREE || get(aIndex).getRange() == aBlockPointer.getRange() : get(aIndex).getBlockType() + " " + get(aIndex).getRange() +"=="+ aBlockPointer.getRange();

		set(aIndex, aBlockPointer);
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.FREE)
		{
			return null;
		}

		BlockPointer blockPointer = get(aIndex);

		assert blockPointer.getRange() != 0;

		return blockPointer;
	}


	int findPointer(int aIndex)
	{
		for (; getPointerType(aIndex) == BlockType.FREE; aIndex--)
		{
		}

		return aIndex;
	}


	void split(int aIndex, BlockPointer aLowPointer, BlockPointer aHighPointer)
	{
		assert aLowPointer.getRange() + aHighPointer.getRange() == get(aIndex).getRange();
		assert ensureEmpty(aIndex + 1, aLowPointer.getRange() + aHighPointer.getRange() - 1);

		set(aIndex, aLowPointer);
		set(aIndex + aLowPointer.getRange(), aHighPointer);
	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = get(aIndex);
		BlockPointer bp2 = get(aIndex + aBlockPointer.getRange());

		assert bp1.getRange() + bp2.getRange() == aBlockPointer.getRange();
		assert ensureEmpty(aIndex + 1, bp1.getRange() - 1);
		assert ensureEmpty(aIndex + bp1.getRange() + 1, bp2.getRange() - 1);

		set(aIndex, aBlockPointer);
		set(aIndex + bp1.getRange(), EMPTY_POINTER);
	}


	private boolean ensureEmpty(int aIndex, int aRange)
	{
		byte[] array = mBuffer;

		for (int i = aIndex * BlockPointer.SIZE, limit = (aIndex + aRange) * BlockPointer.SIZE; i < limit; i++)
		{
			if (array[i] != 0)
			{
				return false;
			}
		}

		return true;
	}


	private BlockType getPointerType(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : "0 >= " + aIndex + " < " + mPointerCount;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private BlockPointer get(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		return new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	private void set(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		aBlockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	String integrityCheck()
	{
		int rangeRemain = 0;

		for (int i = 0; i < mPointerCount; i++)
		{
			BlockPointer bp = get(i);

			if (rangeRemain > 0)
			{
				if (bp.getRange() != 0)
				{
					return "Pointer inside range";
				}
			}
			else
			{
				if (bp.getRange() == 0)
				{
					return "Zero range";
				}
				rangeRemain = bp.getRange();
			}
			rangeRemain--;
		}

		return null;
	}


	void gc()
	{
		if (mGCEnabled)
		{
			mBuffer = null;
			mPointerCount = 0;
		}
	}


	HashTableNode setGCEnabled(boolean aGCEnabled)
	{
		mGCEnabled = aGCEnabled;
		return this;
	}


	boolean getValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry)
	{
		assert mHashTable.mPerformanceTool.tick("getValue");

		Log.i("get %s value", mHashTable.mTableName);

		mHashTable.mCost.mTreeTraversal++;

		BlockPointer blockPointer = getPointer(findPointer(mHashTable.computeIndex(aKey, aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return mHashTable.readNode(blockPointer).getValue(aKey, aLevel + 1, aEntry);
			case LEAF:
				mHashTable.mCost.mValueGet++;
				HashTableLeaf leaf = mHashTable.readLeaf(blockPointer);
				boolean result = leaf.get(aEntry);
				leaf.gc();
				return result;
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	byte[] putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("putValue");

		Log.d("put %s value", mHashTable.mTableName);
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = readNode(blockPointer);
				oldValue = node.putValue(aEntry, aKey, aLevel + 1);
				mHashTable.freeBlock(blockPointer);
				setPointer(index, mHashTable.writeBlock(node, blockPointer.getRange()));
				node.gc();
				break;
			case LEAF:
				oldValue = putValueLeaf(blockPointer, index, aEntry, aLevel, aKey);
				break;
			case HOLE:
				oldValue = upgradeHoleToLeaf(aEntry, blockPointer, index);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	private byte[] putValueLeaf(BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel, byte[] aKey)
	{
		assert mHashTable.mPerformanceTool.tick("putValueLeaf");

		mHashTable.mCost.mTreeTraversal++;

		HashTableLeaf map = readLeaf(aBlockPointer);

		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.getValue();

			mHashTable.freeBlock(aBlockPointer);

			setPointer(aIndex, mHashTable.writeBlock(map, aBlockPointer.getRange()));

			mHashTable.mCost.mValuePut++;
		}
		else if (map.splitLeaf(aBlockPointer, aIndex, aLevel, this))
		{
			oldValue = putValue(aEntry, aKey, aLevel); // recursive put
		}
		else
		{
			HashTableNode node = map.splitLeaf(aBlockPointer, aLevel + 1);

			oldValue = node.putValue(aEntry, aKey, aLevel + 1); // recursive put

			setPointer(aIndex, mHashTable.writeBlock(node, aBlockPointer.getRange()));

			node.gc();
		}

		return oldValue;
	}


	HashTableLeaf readLeaf(BlockPointer aBlockPointer)
	{
		return new HashTableLeaf(mHashTable, mHashTable.readBlock(aBlockPointer));
	}


	HashTableNode readNode(BlockPointer aBlockPointer)
	{
		return new HashTableNode(mHashTable, mHashTable.readBlock(aBlockPointer));
	}


	byte[] upgradeHoleToLeaf(ArrayMapEntry aEntry, BlockPointer aBlockPointer, int aIndex)
	{
		assert mHashTable.mPerformanceTool.tick("upgradeHoleToLeaf");

		Log.d("upgrade hole to leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		HashTableLeaf node = new HashTableLeaf(mHashTable, mHashTable.mLeafSize);

		if (!node.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		BlockPointer blockPointer = mHashTable.writeBlock(node, aBlockPointer.getRange());
		setPointer(aIndex, blockPointer);

		node.gc();

		Log.dec();

		return oldValue;
	}


	boolean removeValue(byte[] aKey, int aLevel, ArrayMapEntry aEntry)
	{
		assert mHashTable.mPerformanceTool.tick("removeValue");

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			{
				HashTableNode node = readNode(blockPointer);
				if (node.removeValue(aKey, aLevel + 1, aEntry))
				{
					mHashTable.freeBlock(blockPointer);
					BlockPointer newBlockPointer = mHashTable.writeBlock(node, blockPointer.getRange());
					setPointer(index, newBlockPointer);
					return true;
				}
				return false;
			}
			case LEAF:
			{
				HashTableLeaf node = readLeaf(blockPointer);
				boolean found = node.remove(aEntry);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;

					mHashTable.freeBlock(blockPointer);
					BlockPointer newBlockPointer = mHashTable.writeBlock(node, blockPointer.getRange());
					setPointer(index, newBlockPointer);
				}

				node.gc();
				return found;
			}
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	void visitNode(HashTableVisitor aVisitor)
	{
		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			BlockPointer next = getPointer(i);

			if (next != null && next.getBlockType() == BlockType.INDEX)
			{
				readNode(next).visitNode(aVisitor);
			}

			aVisitor.visit(i, next);
		}

		gc();
	}
}
