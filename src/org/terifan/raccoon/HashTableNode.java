package org.terifan.raccoon;

import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;


final class HashTableNode implements Node
{
	private byte[] mBuffer;
	private int mPointerCount;
	private boolean mGCEnabled;
	private HashTable mHashTable;
	private BlockPointer mBlockPointer;


	public HashTableNode(HashTable aHashTable)
	{
		mHashTable = aHashTable;
		mPointerCount = mHashTable.mNodeSize / BlockPointer.SIZE;
		mBuffer = new byte[mHashTable.mNodeSize];
		mGCEnabled = true;
	}


	public HashTableNode(HashTable aHashTable, BlockPointer aBlockPointer)
	{
		mHashTable = aHashTable;
		mBuffer = mHashTable.readBlock(aBlockPointer);
		mPointerCount = mBuffer.length / BlockPointer.SIZE;
		mGCEnabled = true;
		mBlockPointer = aBlockPointer;

		assert mHashTable.mPerformanceTool.tick("readNode");

		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		mHashTable.mCost.mReadBlockNode++;
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


	BlockPointer getPointer(int aIndex)
	{
		if (isFree(aIndex))
		{
			return null;
		}

		BlockPointer blockPointer = get(aIndex);

		assert blockPointer.getRange() != 0;

		return blockPointer;
	}


	int findPointer(int aIndex)
	{
		for (; isFree(aIndex); aIndex--)
		{
		}

		return aIndex;
	}


//	void merge(int aIndex, BlockPointer aBlockPointer)
//	{
//		BlockPointer bp1 = get(aIndex);
//		BlockPointer bp2 = get(aIndex + aBlockPointer.getRange());
//
//		assert bp1.getRange() + bp2.getRange() == aBlockPointer.getRange();
//		assert ensureEmpty(aIndex + 1, bp1.getRange() - 1);
//		assert ensureEmpty(aIndex + bp1.getRange() + 1, bp2.getRange() - 1);
//
//		set(aIndex, aBlockPointer);
//		set(aIndex + bp1.getRange(), EMPTY_POINTER);
//	}


	private boolean isFree(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount : "0 >= " + aIndex + " < " + mPointerCount;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE) == BlockType.FREE;
	}


	BlockPointer get(int aIndex)
	{
		assert aIndex >= 0 && aIndex < mPointerCount;

		return new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
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
				return new HashTableNode(mHashTable, blockPointer).getValue(aKey, aLevel + 1, aEntry);
			case LEAF:
				mHashTable.mCost.mValueGet++;
				HashTableLeaf leaf = new HashTableLeaf(mHashTable, blockPointer);
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
				HashTableNode node = new HashTableNode(mHashTable, blockPointer);
				oldValue = node.putValue(aEntry, aKey, aLevel + 1);
				mHashTable.freeBlock(blockPointer);
				writeBlock(index, node, blockPointer.getRange());
				node.gc();
				break;
			case LEAF:
				HashTableLeaf leaf = new HashTableLeaf(mHashTable, blockPointer);
				oldValue = leaf.putValueLeaf(this, index, aEntry, aLevel, aKey);
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


	byte[] upgradeHoleToLeaf(ArrayMapEntry aEntry, BlockPointer aBlockPointer, int aIndex)
	{
		assert mHashTable.mPerformanceTool.tick("upgradeHoleToLeaf");

		Log.d("upgrade hole to leaf");
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		HashTableLeaf node = new HashTableLeaf(mHashTable);

		if (!node.put(aEntry))
		{
			throw new DatabaseException("Failed to upgrade hole to leaf");
		}

		byte[] oldValue = aEntry.getValue();

		writeBlock(aIndex, node, aBlockPointer.getRange());

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
				HashTableNode node = new HashTableNode(mHashTable, blockPointer);
				if (node.removeValue(aKey, aLevel + 1, aEntry))
				{
					mHashTable.freeBlock(blockPointer);
					writeBlock(index, node, blockPointer.getRange());
					return true;
				}
				return false;
			case LEAF:
				HashTableLeaf leaf = new HashTableLeaf(mHashTable, blockPointer);
				boolean found = leaf.remove(aEntry);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;

					mHashTable.freeBlock(blockPointer);
					writeBlock(index, leaf, blockPointer.getRange());
				}

				leaf.gc();
				return found;
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
				HashTableNode node = new HashTableNode(mHashTable, next);
				node.visitNode(aVisitor);
			}

			aVisitor.visit(i, next);
		}

		gc();
	}


	void writeBlock(int aIndex, Node aNode, int aRange)
	{
		BlockPointer blockPointer;

		if (aNode instanceof HashTableLeaf && ((HashTableLeaf)aNode).isEmpty())
		{
			blockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}
		else
		{
			assert aIndex >= 0 && aIndex < mPointerCount;

			blockPointer = mHashTable.writeBlock(aNode, aRange);
		}

		blockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}
}
