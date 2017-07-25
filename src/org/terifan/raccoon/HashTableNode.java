package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


final class HashTableNode extends Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private byte[] mBuffer;
	private int mPointerCount;
	private boolean mGCEnabled;
	private HashTable mHashTable;
	private BlockPointer mBlockPointer;


	public HashTableNode(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mBuffer = aHashTable.getBlockAccessor().readBlock(aBlockPointer);
		mPointerCount = mBuffer.length / BlockPointer.SIZE;
		mGCEnabled = true;
		mHashTable = aHashTable;
		mBlockPointer = aBlockPointer;
	}


	public HashTableNode(HashTable aHashTable)
	{
		mHashTable = aHashTable;
		mBuffer = new byte[mHashTable.getNodeSize()];
		mPointerCount = mHashTable.getNodeSize() / BlockPointer.SIZE;
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
		assert aIndex >= 0 && aIndex < mPointerCount;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private BlockPointer get(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		return new BlockPointer().unmarshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	private void set(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		aBlockPointer.marshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
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
		Log.i("get %s value", mHashTable.getTableName());

		BlockPointer blockPointer = getPointer(findPointer(mHashTable.computeIndex(aKey, aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return mHashTable.readNode(blockPointer).getValue(aKey, aLevel + 1, aEntry);
			case LEAF:
				return mHashTable.readLeaf(blockPointer).get(aEntry);
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	void freeBlock()
	{
		mHashTable.getBlockAccessor().freeBlock(mBlockPointer);
	}


	byte[] readBlock()
	{
		return mHashTable.getBlockAccessor().readBlock(mBlockPointer);
	}


	BlockPointer writeBlock(int aRange)
	{
		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId().get(), getType(), aRange);
		return mBlockPointer;
	}


	boolean remove(byte[] aKey, int aLevel, ArrayMapEntry aEntry)
	{
		int index = findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = getPointer(index);
		BlockPointer newBlockPointer = null;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = mHashTable.readNode(blockPointer);
				if (node.remove(aKey, aLevel + 1, aEntry))
				{
					node.freeBlock();
					newBlockPointer = node.writeBlock(blockPointer.getRange());
				}
				break;
			case LEAF:
				HashTableLeaf leaf = mHashTable.readLeaf(blockPointer);
				if (leaf.remove(aEntry))
				{
					leaf.freeBlock();
					newBlockPointer = leaf.writeBlock(blockPointer.getRange());
				}
				break;
			case HOLE:
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		if (newBlockPointer != null)
		{
			setPointer(index, newBlockPointer);
		}

		return newBlockPointer != null;
	}


	byte[] putValue(ArrayMapEntry aEntry, byte[] aKey, int aLevel)
	{
		Log.d("put %s value", mHashTable.getTableName());
		Log.inc();

		int index = findPointer(mHashTable.computeIndex(aKey, aLevel));
		BlockPointer blockPointer = getPointer(index);
		byte[] oldValue;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = mHashTable.readNode(blockPointer);
				oldValue = node.putValue(aEntry, aKey, aLevel + 1);
				node.freeBlock();
				BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRange());
				setPointer(index, newBlockPointer);
				node.gc();
				break;
			case LEAF:
				HashTableLeaf map = mHashTable.readLeaf(blockPointer);
				oldValue = putValueLeaf(map, blockPointer, index, aEntry, aLevel, aKey);
				break;
			case HOLE:
				oldValue = upgradeHoleToLeaf(aEntry, this, blockPointer, index);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return oldValue;
	}


	byte[] putValueLeaf(HashTableLeaf map, BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel, byte[] aKey)
	{
		BlockPointer newBlockPointer;
		byte[] oldValue;

		if (map.put(aEntry))
		{
			oldValue = aEntry.getValue();

			map.freeBlock();

			newBlockPointer = map.writeBlock(aBlockPointer.getRange());
		}
		else if (map.splitLeaf(aIndex, aLevel, this))
		{
			return putValue(aEntry, aKey, aLevel); // recursive put
		}
		else
		{
			HashTableNode node = map.splitLeaf(aLevel + 1);

			oldValue = node.putValue(aEntry, aKey, aLevel + 1); // recursive put

			newBlockPointer = node.writeBlock(aBlockPointer.getRange());
		}

		setPointer(aIndex, newBlockPointer);

		return oldValue;
	}


	private byte[] upgradeHoleToLeaf(ArrayMapEntry aEntry, HashTableNode aParent, BlockPointer aBlockPointer, int aIndex)
	{
		Log.d("upgrade hole to leaf");
		Log.inc();

		HashTableLeaf node = new HashTableLeaf(mHashTable);

		if (!node.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		BlockPointer newBlockPointer = node.writeBlock(aBlockPointer.getRange());

		aParent.setPointer(aIndex, newBlockPointer);

		node.gc();

		Log.dec();

		return oldValue;
	}
}
