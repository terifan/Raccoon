package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.storage.IBlockAccessor;
import org.terifan.raccoon.util.ByteArrayBuffer;


/*

read pointers:    [a][ ][c][ ]       pointers have been requested and have been decoded
read blocks:      [ ][ ][ ][ ]       blocks in memory, blocks pending to be written are removed

pending pointers: [ ][b][c][ ]       new pointers exists, a pointer can point to an already written block
pending blocks:   [ ][ ][c][ ]       blocks pending to be written, some blocks might already have been written

*/
final class HashTableNode extends HashTableAbstractNode
{
	private final static BlockPointer EMPTY_POINTER = new BlockPointer();

	private byte[] mBuffer;
	private int mPointerCount;

	private BlockPointer[] mPointers;
	private HashTableAbstractNode[] mChildren;


	public HashTableNode(HashTable aHashTable, IBlockAccessor aBlockAccessor, HashTableNode aParent, byte[] aBuffer)
	{
		super(aHashTable, aBlockAccessor, aParent, null);

		mBuffer = aBuffer;
		mPointerCount = aBuffer.length / BlockPointer.SIZE;

		mPointers = new BlockPointer[mPointerCount];
		mChildren = new HashTableAbstractNode[mPointerCount];
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
		setPointerImpl(aBlockPointer.getRangeOffset(), aBlockPointer);
	}


	BlockPointer getPointerByHash(long aHashCode)
	{
		return getPointer(findPointerIndex(mHashTable.computeIndex(aHashCode, mBlockPointer.getLevel())));
	}


	BlockPointer getPointer(int aIndex)
	{
		if (getPointerType(aIndex) == BlockType.FREE)
		{
			return null;
		}

		return getPointerImpl(aIndex);
	}


	private int findPointerIndex(int aIndex)
	{
		for (; getPointerType(aIndex) == BlockType.FREE; aIndex--)
		{
		}

		return aIndex;
	}


	void merge(int aIndex, BlockPointer aBlockPointer)
	{
		BlockPointer bp1 = getPointerImpl(aIndex);
		BlockPointer bp2 = getPointerImpl(aIndex + aBlockPointer.getRangeSize());

		assert bp1.getRangeSize() + bp2.getRangeSize() == aBlockPointer.getRangeSize();
		assert ensureEmpty(aIndex + 1, bp1.getRangeSize() - 1);
		assert ensureEmpty(aIndex + bp1.getRangeSize() + 1, bp2.getRangeSize() - 1);

		setPointerImpl(aIndex, aBlockPointer);
		setPointerImpl(aIndex + bp1.getRangeSize(), EMPTY_POINTER);
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

		if (mPointers[aIndex] != null)
		{
			return mPointers[aIndex].getBlockType();
		}

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE);
	}


	private BlockPointer getPointerImpl(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : aIndex;

		if (mPointers[aIndex] == null)
		{
			mPointers[aIndex] = new BlockPointer().unmarshal(new ByteArrayBuffer(mBuffer).position(aIndex * BlockPointer.SIZE));
		}

		return mPointers[aIndex];
	}


	void setPointerImpl(int aIndex, BlockPointer aBlockPointer)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		mFlushAction = FlushAction.WRITE;
		mPointers[aIndex] = aBlockPointer;
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
//		mBlockPointer = mBlockAccessor.writeBlock(array(), 0, array().length, mHashTable.getTransactionId(), getBlockType(), mBlockPointer.getRangeOffset(), mBlockPointer.getRangeSize(), mBlockPointer.getLevel());
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
			BlockPointer bp = getPointerImpl(i);

			if (rangeRemain > 0)
			{
				if (bp.getRangeSize() != 0)
				{
					return "Pointer inside range: ranges: " + toRangesString();
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
		HashTableLeaf leaf = new HashTableLeaf(mHashTable, mBlockAccessor, this, new byte[mHashTable.getLeafSize()]);
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

			mBlockAccessor.freeBlock(mBlockPointer);

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


	HashTableAbstractNode readBlock(BlockPointer aBlockPointer)
	{
		System.out.println("read  " + aBlockPointer);

		int offset = aBlockPointer.getRangeOffset();
		HashTableAbstractNode node = mChildren[offset];

		if (node != null)
		{
			return node;
		}

		if (mChildren[offset] == null)
		{
			if (mPointers[offset] == null) //////// ???
			{
				mPointers[offset] = aBlockPointer;
			}

			switch (aBlockPointer.getBlockType())
			{
				case LEAF:
					mChildren[offset] = new HashTableLeaf(mHashTable, mBlockAccessor, this, mBlockAccessor.readBlock(aBlockPointer));
					break;
				case INDEX:
					mChildren[offset] = new HashTableNode(mHashTable, mBlockAccessor, this, mBlockAccessor.readBlock(aBlockPointer));
					break;
				default:
					throw new IllegalStateException();
			}
		}

		return mChildren[offset];
	}


	public String toRangesString()
	{
		StringBuilder sb = new StringBuilder("[");

		for (int j = 0; j < mPointerCount; j++)
		{
			BlockPointer ptr = getPointerImpl(j);

			if (ptr.getRangeSize() != 0)
			{
				if (sb.length() > 1)
				{
					sb.append(",");
				}

				sb.append(ptr.getRangeSize());
			}
		}

		return sb.append("]").toString();
	}


	public String toPointersString()
	{
		StringBuilder sb = new StringBuilder();

		for (int j = 0; j < mPointerCount; )
		{
			if (sb.length() > 0)
			{
				sb.append(",");
			}

			BlockPointer ptr = getPointerImpl(j);

			sb.append("[");
			if (ptr.getBlockType() == BlockType.LEAF)
			{
				sb.append("*");
			}
			sb.append(ptr.getBlockIndex0());

			j++;

			for (; j < mPointerCount; j++)
			{
				if (getPointerImpl(j).getRangeSize() != 0)
				{
					break;
				}

				sb.append(",0");
			}

			sb.append("]");
		}

		return sb.toString();
	}


	@Override
	public String toString()
	{
		return toRangesString();
	}
}
