package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;


class HashTreeTableNodeArray
{
	private final HashTreeTable mHashTable;
	private final HashTreeTableInnerNode mNode;

	private int mPointerCount;
	private final byte[] mBuffer;
//	private BlockPointer[] mPointers;
//	private HashTableNode[] mNodes;


	public HashTreeTableNodeArray(HashTreeTable aHashTable, HashTreeTableInnerNode aNode, byte[] aBuffer)
	{
		assert (aBuffer.length % BlockPointer.SIZE) == 0;

		mHashTable = aHashTable;
		mNode = aNode;
		mBuffer = aBuffer;

		mPointerCount = mBuffer.length / BlockPointer.SIZE;
//		mPointers = new BlockPointer[mSize];
//		mNodes = new HashTableNode[mSize];
	}


	public <T extends HashTreeTableNode> T getNode(int aIndex)
	{
//		HashTableNode node = mNodes[aIndex];
		HashTreeTableNode node = null;

		if (node == null)
		{
			BlockPointer blockPointer = getPointer(aIndex);

			if (blockPointer != null)
			{
				switch (blockPointer.getBlockType())
				{
					case INDEX:
						node = new HashTreeTableInnerNode(mHashTable, mNode, blockPointer);
						break;
					case LEAF:
						node = new HashTreeTableLeafNode(mHashTable, mNode, blockPointer);
						break;
					case HOLE:
						node = new HashTreeTableLeafNode(mHashTable, mNode);
						break;
					case FREE:
					default:
						throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
				}

//				mNodes[aIndex] = node;
			}
		}

		return (T)node;
	}


	public BlockPointer getPointer(int aIndex)
	{
		if (isFree(aIndex))
		{
			return null;
		}

		assert aIndex >= 0 && aIndex < mPointerCount;

		BlockPointer blockPointer = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));

		assert blockPointer.getUserData() != 0;

		return blockPointer;
	}


	public BlockPointer getPointer2(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		BlockPointer blockPointer = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));

		return blockPointer;
	}


//	public void set(int aIndex, BlockPointer aBlockPointer)
//	{
//		mPointers[aIndex] = aBlockPointer;
//	}


	public boolean isFree(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : "0 >= " + aIndex + " < " + mPointerCount;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE) == BlockType.FREE;
	}


	public byte[] array()
	{
		return mBuffer;
	}


	public void set(int aIndex, HashTreeTableNode aNode, int aRange)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : "0 <= " + aIndex + " < " + mPointerCount;

		BlockPointer blockPointer;

		if (aNode instanceof HashTreeTableLeafNode && ((HashTreeTableLeafNode)aNode).size() == 0)
		{
			blockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setUserData(aRange);
		}
		else
		{
			assert aIndex >= 0 && aIndex < mHashTable.mPointersPerNode;

			blockPointer = mHashTable.writeBlock(aNode, aRange);
		}

		blockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	public void markDirty(int aIndex, HashTreeTableNode aNode, int aRange)
	{
		mHashTable.freeBlock(getPointer(aIndex));

		writeBlock(aIndex, aNode, aRange);
	}


	public void writeBlock(int aIndex, HashTreeTableNode aNode, int aRange)
	{
		BlockPointer blockPointer;

		if (aNode instanceof HashTreeTableLeafNode && ((HashTreeTableLeafNode)aNode).size() == 0)
		{
			blockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setUserData(aRange);
		}
		else
		{
			assert aIndex >= 0 && aIndex < mHashTable.mPointersPerNode;

			blockPointer = mHashTable.writeBlock(aNode, aRange);
		}

		blockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	public void freePointer(int aIndex)
	{
		BlockPointer bp = new BlockPointer().setBlockType(BlockType.FREE);

		bp.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	void freeNode(HashTreeTableNode aNode)
	{
		BlockPointer bp = aNode.getBlockPointer();

		if (bp != null && bp.getBlockType() != BlockType.HOLE && bp.getBlockType() != BlockType.FREE)
		{
			mHashTable.freeBlock(bp);
		}
	}
}
