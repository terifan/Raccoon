package org.terifan.raccoon;

import java.util.HashMap;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


final class HashTableNode extends Node
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private byte[] mBuffer;
	private int mPointerCount;

	private BlockPointer[] mPointers;
	private Node[] mChildren;


	public HashTableNode(HashTable aHashTable, HashTableNode aParent, byte[] aBuffer)
	{
		super(aHashTable, aParent, null);

		mBuffer = aBuffer;
		mPointerCount = aBuffer.length / BlockPointer.SIZE;

		mPointers = new BlockPointer[mPointerCount];
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
		set(aBlockPointer.getRangeOffset(), aBlockPointer);
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
		assert aIndex >= 0 && aIndex < mPointerCount : aIndex;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private BlockPointer get(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : aIndex;

		if (mPointers[aIndex] == null)
		{
			mPointers[aIndex] = new BlockPointer().unmarshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
		}

		return mPointers[aIndex];
	}


	void set(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		mPointers[aIndex] = aBlockPointer;
	}


	Node getChildNode(BlockPointer aBlockPointer)
	{
		int i = 0;

		if (mPointers[i] == null)
		{
			mPointers[i] = aBlockPointer;

			if (aBlockPointer.getBlockType() == BlockType.LEAF)
			{
				mChildren[i] = new HashTableLeaf(mHashTable, this, mHashTable.getBlockAccessor().readBlock(aBlockPointer));
			}
			else
			{
				mChildren[i] = new HashTableNode(mHashTable, this, mHashTable.getBlockAccessor().readBlock(aBlockPointer));
			}
		}

		return mChildren[i];
	}


	@Override
	void flush()
	{
		for (int i = 0; i < mPointers.length; i++)
		{
			if (mPointers[i] != null)
			{
				mPointers[i].marshal(new ByteArrayBuffer(mBuffer).position(i * BlockPointer.SIZE));

				mPointers[i] = null;
			}
		}

		writeBlock();
	}


//	@Override
//	void flush()
//	{
//		for (Node node : mChildren.values())
//		{
//			node.flush();
//		}
//
//		mBlockPointer = mHashTable.getBlockAccessor().writeBlock(array(), 0, array().length, mHashTable.getTransactionId(), getBlockType(), mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
//
//		System.out.println("flush   " + mBlockPointer);
//
//		if (mParent != null)
//		{
//			mParent.mChildren.put(mBlockPointer.getRangeOffset(), this);
//			mParent.setPointer(mBlockPointer);
//		}
//	}


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


	@Deprecated
	HashTableLeaf upgrade(BlockPointer aBlockPointer, ArrayMapEntry aEntry)
	{
		HashTableLeaf leaf = new HashTableLeaf(mHashTable, this, new byte[mHashTable.getLeafSize()]);
		leaf.setBlockPointer(new BlockPointer().setLevel(aBlockPointer.getLevel()).setRangeOffset(aBlockPointer.getRangeOffset()).setRangeSize(aBlockPointer.getRangeSize()));

		if (!leaf.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		leaf.writeBlock();

		return leaf;
	}


	@Override
	void freeBlock()
	{
		if (mBlockPointer.getBlockType() != BlockType.FREE && mBlockPointer.getBlockType() != BlockType.PENDING_WRITE)
		{
			System.out.println("free    " + mBlockPointer);

			mHashTable.getBlockAccessor().freeBlock(mBlockPointer);

			mBlockPointer.setBlockType(BlockType.FREE);

			mPointers[mBlockPointer.getRangeOffset()] = null;
			mChildren[mBlockPointer.getRangeOffset()] = null;
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


	Node readBlock(BlockPointer aBlockPointer)
	{
		System.out.println("read  " + aBlockPointer);

		Node node = mChildren[aBlockPointer.getRangeOffset()];

		if (node == null)
		{
			node = getChildNode(aBlockPointer);

			mChildren[aBlockPointer.getRangeOffset()] = node;
		}

		return node;
	}
}
