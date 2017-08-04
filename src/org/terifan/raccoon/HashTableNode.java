package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


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


	void setPointer(BlockPointer aBlockPointer)
	{
		int index = aBlockPointer.getRangeOffset();

//		assert get(index).getBlockType() == BlockType.FREE || (get(index).getRangeSize() == aBlockPointer.getRangeSize() && get(index).getRangeOffset() == index) : get(index).getBlockType() + " " + get(index).getRangeOffset()+":"+get(index).getRangeSize() + " != " + index+":"+aBlockPointer.getRangeSize();

		set(index, aBlockPointer);
	}


	BlockPointer getPointerByHash(long aHashCode)
	{
		return getPointer(findPointer(mHashTable.computeIndex(aHashCode, mBlockPointer.getLevel())));
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.FREE)
		{
			return null;
		}

		return get(aIndex);
	}


	int findPointer(int aIndex)
	{
		for (; getPointerType(aIndex) == BlockType.FREE; aIndex--)
		{
		}

		return aIndex;
	}


//	void split(int aIndex, BlockPointer aLowPointer, BlockPointer aHighPointer)
//	{
//		assert aLowPointer.getRangeSize() + aHighPointer.getRangeSize() == get(aIndex).getRangeSize();
//		assert ensureEmpty(aIndex + 1, aLowPointer.getRangeSize() + aHighPointer.getRangeSize() - 1);
//
//		set(aIndex, aLowPointer);
//		set(aIndex + aLowPointer.getRangeSize(), aHighPointer);
//	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = get(aIndex);
		BlockPointer bp2 = get(aIndex + aBlockPointer.getRangeSize());

		assert bp1.getRangeSize() + bp2.getRangeSize() == aBlockPointer.getRangeSize();
		assert ensureEmpty(aIndex + 1, bp1.getRangeSize() - 1);
		assert ensureEmpty(aIndex + bp1.getRangeSize() + 1, bp2.getRangeSize() - 1);

		set(aIndex, aBlockPointer);
		set(aIndex + bp1.getRangeSize(), EMPTY_POINTER);
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


	void set(int aIndex, BlockPointer aBlockPointer)
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
				if (bp.getRangeSize() != 0)
				{
					return "Pointer inside range: ranges: " + printRanges();
				}
			}
			else
			{
				if (bp.getRangeSize() == 0)
				{
					return "Zero range";
				}
				rangeRemain = bp.getRangeSize();
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

			sb.append(get(j).getRangeSize());
		}

		return sb.toString();
	}


	@Override
	boolean remove(ArrayMapEntry aEntry)
	{
		assert false;
		return false;
//		int index = findPointer(mHashTable.computeIndex(aEntry.getKey(), aLevel));
//		BlockPointer blockPointer = getPointer(index);
//		BlockPointer newBlockPointer = null;
//
//		switch (blockPointer.getBlockType())
//		{
//			case INDEX:
//				HashTableNode node = mHashTable.readNode(blockPointer, this);
//				if (node.remove(aEntry, aLevel + 1))
//				{
//					node.freeBlock();
//					newBlockPointer = node.writeBlock();
//				}
//				break;
//			case LEAF:
//				HashTableLeaf leaf = mHashTable.readLeaf(blockPointer, this);
//				if (leaf.remove(aEntry, aLevel + 1))
//				{
//					leaf.freeBlock();
//					newBlockPointer = leaf.writeBlock();
//				}
//				break;
//			case HOLE:
//				break;
//			case FREE:
//			default:
//				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
//		}
//
//		if (newBlockPointer != null)
//		{
//			setPointer(newBlockPointer);
//		}
//
//		return newBlockPointer != null;
	}


	HashTableLeaf upgrade(BlockPointer aBlockPointer, ArrayMapEntry aEntry)
	{
		HashTableLeaf leaf = new HashTableLeaf(mHashTable, this);
		leaf.setBlockPointer(new BlockPointer().setLevel(aBlockPointer.getLevel()).setRangeOffset(aBlockPointer.getRangeOffset()).setRangeSize(aBlockPointer.getRangeSize()));

		if (!leaf.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		leaf.writeBlock();
		
		return leaf;
	}
}
