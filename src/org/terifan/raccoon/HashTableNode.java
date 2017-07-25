package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


final class HashTableNode extends Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private byte[] mBuffer;
	private int mPointerCount;


	public HashTableNode(HashTable aHashTable, HashTableNode aParent, BlockPointer aBlockPointer)
	{
		super(aHashTable, aParent, aBlockPointer);

		mBuffer = aHashTable.getBlockAccessor().readBlock(aBlockPointer);
		mPointerCount = mBuffer.length / BlockPointer.SIZE;
	}


	public HashTableNode(HashTable aHashTable, HashTableNode aParent)
	{
		super(aHashTable, aParent, null);

		mBuffer = new byte[mHashTable.getNodeSize()];
		mPointerCount = mHashTable.getNodeSize() / BlockPointer.SIZE;
	}


	@Override
	public byte[] array()
	{
		return mBuffer;
	}


	@Override
	public BlockType getBlockType()
	{
		return BlockType.INDEX;
	}


	int getPointerCount()
	{
		return mPointerCount;
	}


	void setPointer(int aIndex, BlockPointer aBlockPointer)
	{
		assert HashTableNode.this.get(aIndex).getBlockType() == BlockType.FREE || HashTableNode.this.get(aIndex).getRange() == aBlockPointer.getRange() : HashTableNode.this.get(aIndex).getBlockType() + " " + HashTableNode.this.get(aIndex).getRange() + "==" + aBlockPointer.getRange();

		set(aIndex, aBlockPointer);
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.FREE)
		{
			return null;
		}

		BlockPointer blockPointer = HashTableNode.this.get(aIndex);

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
		assert aLowPointer.getRange() + aHighPointer.getRange() == HashTableNode.this.get(aIndex).getRange();
		assert ensureEmpty(aIndex + 1, aLowPointer.getRange() + aHighPointer.getRange() - 1);

		set(aIndex, aLowPointer);
		set(aIndex + aLowPointer.getRange(), aHighPointer);
	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = HashTableNode.this.get(aIndex);
		BlockPointer bp2 = HashTableNode.this.get(aIndex + aBlockPointer.getRange());

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


	@Override
	String integrityCheck()
	{
		int rangeRemain = 0;

		for (int i = 0; i < mPointerCount; i++)
		{
			BlockPointer bp = HashTableNode.this.get(i);

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


	@Override
	boolean get(ArrayMapEntry aEntry, int aLevel)
	{
		Log.i("get %s value", mHashTable.getTableName());

		BlockPointer blockPointer = getPointer(findPointer(mHashTable.computeIndex(aEntry.getKey(), aLevel)));

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return mHashTable.readNode(blockPointer, this).get(aEntry, aLevel + 1);
			case LEAF:
				return mHashTable.readLeaf(blockPointer, this).get(aEntry, aLevel + 1);
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	@Override
	void freeBlock()
	{
		mHashTable.getBlockAccessor().freeBlock(mBlockPointer);
	}


	byte[] readBlock()
	{
		return mHashTable.getBlockAccessor().readBlock(mBlockPointer);
	}


	@Override
	BlockPointer writeBlock(int aRange)
	{
		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId().get(), getBlockType(), aRange);
		return mBlockPointer;
	}


	@Override
	boolean remove(ArrayMapEntry aEntry, int aLevel)
	{
		int index = findPointer(mHashTable.computeIndex(aEntry.getKey(), aLevel));
		BlockPointer blockPointer = getPointer(index);
		BlockPointer newBlockPointer = null;

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = mHashTable.readNode(blockPointer, this);
				if (node.remove(aEntry, aLevel + 1))
				{
					node.freeBlock();
					newBlockPointer = node.writeBlock(blockPointer.getRange());
				}
				break;
			case LEAF:
				HashTableLeaf leaf = mHashTable.readLeaf(blockPointer, this);
				if (leaf.remove(aEntry, aLevel + 1))
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


	@Override
	boolean put(ArrayMapEntry aEntry, int aLevel)
	{
		Log.d("put %s value", mHashTable.getTableName());
		Log.inc();

		int index = findPointer(mHashTable.computeIndex(aEntry.getKey(), aLevel));
		BlockPointer blockPointer = getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableNode node = mHashTable.readNode(blockPointer, this);
				node.put(aEntry, aLevel + 1);
				node.freeBlock();
				BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRange());
				setPointer(index, newBlockPointer);
				break;
			case LEAF:
				HashTableLeaf map = mHashTable.readLeaf(blockPointer, this);
				putValueLeaf(map, blockPointer, index, aEntry, aLevel);
				break;
			case HOLE:
				upgradeHoleToLeaf(aEntry, this, blockPointer, index, aLevel);
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return true;
	}


	void putValueLeaf(HashTableLeaf aLeaf, BlockPointer aBlockPointer, int aIndex, ArrayMapEntry aEntry, int aLevel)
	{
		BlockPointer newBlockPointer;

		if (aLeaf.put(aEntry, aLevel))
		{
			aLeaf.freeBlock();

			newBlockPointer = aLeaf.writeBlock(aBlockPointer.getRange());
		}
		else if (aLeaf.splitLeaf(aIndex, aLevel, this))
		{
			put(aEntry, aLevel); // recursive put
			return;
		}
		else
		{
			HashTableNode newNode = aLeaf.splitLeaf(aLevel + 1);

			newNode.put(aEntry, aLevel + 1); // recursive put

			newBlockPointer = newNode.writeBlock(aBlockPointer.getRange());
		}

		setPointer(aIndex, newBlockPointer);
	}


	private void upgradeHoleToLeaf(ArrayMapEntry aEntry, HashTableNode aParent, BlockPointer aBlockPointer, int aIndex, int aLevel)
	{
		Log.d("upgrade hole to leaf");
		Log.inc();

		HashTableLeaf node = new HashTableLeaf(mHashTable, aParent);

		if (!node.put(aEntry, aLevel))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		BlockPointer newBlockPointer = node.writeBlock(aBlockPointer.getRange());

		aParent.setPointer(aIndex, newBlockPointer);

		Log.dec();
	}
}
