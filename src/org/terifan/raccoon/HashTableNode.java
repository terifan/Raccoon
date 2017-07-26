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
		assert get(aIndex).getBlockType() == BlockType.FREE || get(aIndex).getRangeOffset() == aBlockPointer.getRangeOffset() : get(aIndex).getBlockType() + " " + get(aIndex).getRangeOffset() + "==" + aBlockPointer.getRangeOffset();

		set(aIndex, aBlockPointer);
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.FREE)
		{
			return null;
		}

		BlockPointer blockPointer = get(aIndex);

		assert blockPointer.getRangeOffset() != 0;
		assert blockPointer.getRangeSize() != 0;

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
		assert aLowPointer.getRangeOffset() + aHighPointer.getRangeOffset() == get(aIndex).getRangeOffset();
		assert ensureEmpty(aIndex + 1, aLowPointer.getRangeOffset() + aHighPointer.getRangeOffset() - 1);

		set(aIndex, aLowPointer);
		set(aIndex + aLowPointer.getRangeOffset(), aHighPointer);
	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = get(aIndex);
		BlockPointer bp2 = get(aIndex + aBlockPointer.getRangeOffset());

		assert bp1.getRangeOffset() + bp2.getRangeOffset() == aBlockPointer.getRangeOffset();
		assert ensureEmpty(aIndex + 1, bp1.getRangeOffset() - 1);
		assert ensureEmpty(aIndex + bp1.getRangeOffset() + 1, bp2.getRangeOffset() - 1);

		set(aIndex, aBlockPointer);
		set(aIndex + bp1.getRangeOffset(), EMPTY_POINTER);
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
			BlockPointer bp = get(i);

			if (rangeRemain > 0)
			{
				if (bp.getRangeOffset() != 0)
				{
					return "Pointer inside range: ranges: " + printRanges();
				}
			}
			else
			{
				if (bp.getRangeOffset() == 0)
				{
					return "Zero range";
				}
				rangeRemain = bp.getRangeOffset();
			}
			rangeRemain--;
		}

		return null;
	}


	String printRanges()
	{
		StringBuilder sb = new StringBuilder();
		for (int j = 0; j < mPointerCount; j++)
		{
			if (j > 0)
			{
				sb.append(", ");
			}
			sb.append(get(j).getRangeOffset());
		}

		return sb.toString();
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


	byte[] readBlock()
	{
		return mHashTable.getBlockAccessor().readBlock(mBlockPointer);
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
					newBlockPointer = node.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());
				}
				break;
			case LEAF:
				HashTableLeaf leaf = mHashTable.readLeaf(blockPointer, this);
				if (leaf.remove(aEntry, aLevel + 1))
				{
					leaf.freeBlock();
					newBlockPointer = leaf.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());
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
				BlockPointer newBlockPointer = node.writeBlock(blockPointer.getRangeOffset(), blockPointer.getRangeSize());
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

			newBlockPointer = aLeaf.writeBlock(aBlockPointer.getRangeOffset(), aBlockPointer.getRangeSize());
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

			newBlockPointer = newNode.writeBlock(aBlockPointer.getRangeOffset(), aBlockPointer.getRangeSize());
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

		BlockPointer newBlockPointer = node.writeBlock(aBlockPointer.getRangeOffset(), aBlockPointer.getRangeSize());

		aParent.setPointer(aIndex, newBlockPointer);

		Log.dec();
	}
}
