package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


final class HashTableInnerNode implements HashTableNode
{
	private byte[] mBuffer;
	private HashTable mHashTable;
	private BlockPointer mBlockPointer;
	private PointerArray mBlockPointers;
	private HashTableInnerNode mParentNode;


	public HashTableInnerNode(HashTable aHashTable, HashTableInnerNode aParent)
	{
		mHashTable = aHashTable;
		mParentNode = aParent;

		mBuffer = new byte[mHashTable.mNodeSize];
		mBlockPointers = new PointerArray(mHashTable.mPointersPerNode);
	}


	public HashTableInnerNode(HashTable aHashTable, HashTableInnerNode aParentNode, BlockPointer aBlockPointer)
	{
		assert aHashTable.mPerformanceTool.tick("readNode");
		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mBlockPointer = aBlockPointer;

		mBuffer = mHashTable.readBlock(mBlockPointer);
		mBlockPointers = new PointerArray(mHashTable.mPointersPerNode);

		mHashTable.mCost.mReadBlockNode++;
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
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


	private BlockPointer getPointer(int aIndex)
	{
		if (isFree(aIndex))
		{
			return null;
		}

		assert aIndex >= 0 && aIndex < mHashTable.mPointersPerNode;

		BlockPointer blockPointer = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));

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
		assert aIndex >= 0 && aIndex < mHashTable.mPointersPerNode : "0 >= " + aIndex + " < " + mHashTable.mPointersPerNode;

		return BlockPointer.getBlockType(mBuffer, aIndex * BlockPointer.SIZE) == BlockType.FREE;
	}


	@Override
	public String integrityCheck()
	{
		int rangeRemain = 0;

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(mBuffer).position(i * BlockPointer.SIZE));

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
	public boolean getValue(ArrayMapEntry aEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("getValue");

		Log.i("get %s value", mHashTable.mTableName);

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			case LEAF:
				HashTableNode node = getNode(index);
				return node.getValue(aEntry, aHash, aLevel + 1);
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	@Override
	public boolean putValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("putValue");

		Log.d("put %s value", mHashTable.mTableName);
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableInnerNode node = getNode(index);
				node.putValue(aEntry, oOldEntry, aHash, aLevel + 1);
				freeBlock(index, node);
				writeBlock(index, node, blockPointer.getRange());
				break;
			case LEAF:
			{
				HashTableLeafNode leaf = getNode(index);
				leaf.putValueLeaf(this, index, aEntry, oOldEntry, aHash, aLevel);
				break;
			}
			case HOLE:
				HashTableLeafNode leaf = getNode(index);
				leaf.upgradeHole(this, index, aEntry, aLevel, blockPointer.getRange());
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return true;
	}


	@Override
	public boolean removeValue(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("removeValue");

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableInnerNode node = getNode(index);
				if (node.removeValue(aEntry, oOldEntry, aHash, aLevel + 1))
				{
					freeBlock(index, node);
					writeBlock(index, node, blockPointer.getRange());
					return true;
				}
				return false;
			case LEAF:
				HashTableLeafNode leaf = getNode(index);
				boolean found = leaf.remove(aEntry, oOldEntry);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;

					freeBlock(index, leaf);
					writeBlock(index, leaf, blockPointer.getRange());
				}

				return found;
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	@Override
	public void visit(HashTableVisitor aVisitor)
	{
		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTableNode node = getNode(i);

			if (node != null)
			{
				node.visit(aVisitor);
			}
		}

		aVisitor.visit(this);
	}


	private void freeBlock(int aIndex, HashTableNode aNode)
	{
		mHashTable.freeBlock(getPointer(aIndex));
	}


	void writeBlock(int aIndex, HashTableNode aNode, int aRange)
	{
		BlockPointer blockPointer;

		if (aNode instanceof HashTableLeafNode && ((HashTableLeafNode)aNode).isEmpty())
		{
			blockPointer = new BlockPointer().setBlockType(BlockType.HOLE).setRange(aRange);
		}
		else
		{
			assert aIndex >= 0 && aIndex < mHashTable.mPointersPerNode;

			blockPointer = mHashTable.writeBlock(aNode, aRange);
		}

		blockPointer.marshal(ByteArrayBuffer.wrap(mBuffer).position(aIndex * BlockPointer.SIZE));
	}


	private <T extends HashTableNode> T getNode(int aIndex)
	{
		BlockPointer blockPointer = getPointer(aIndex);

		if (blockPointer == null)
		{
			return null;
		}

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				return (T)new HashTableInnerNode(mHashTable, this, blockPointer);
			case LEAF:
				return (T)new HashTableLeafNode(mHashTable, this, blockPointer);
			case HOLE:
				return (T)new HashTableLeafNode(mHashTable, this);
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		aScanResult.enterInnerNode(mBlockPointer);
		aScanResult.innerNodes++;

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTableNode node = getNode(i);

			if (node != null)
			{
				node.scan(aScanResult);
			}
		}

		aScanResult.exitInnerNode();
	}


	public Iterator<HashTableNode> iterator()
	{
		return new Iterator<HashTableNode>()
		{
			int index;
			HashTableNode next;


			@Override
			public boolean hasNext()
			{
				if (next == null)
				{
					if (index >= mHashTable.mPointersPerNode)
					{
						return false;
					}

					while (next == null && index < mHashTable.mPointersPerNode)
					{
						next = getNode(index++);
					}
				}

				return next != null;
			}


			@Override
			public HashTableNode next()
			{
				if (next == null)
				{
					throw new IllegalStateException();
				}
				HashTableNode tmp = next;
				next = null;
				return tmp;
			}
		};
	}


	@Override
	public BlockPointer flush()
	{
		if (mBlockPointer != null)
		{
			mHashTable.freeBlock(mBlockPointer);
		}

		mBlockPointer = mHashTable.writeBlock(this, mHashTable.mPointersPerNode);

		return mBlockPointer;
	}
}
