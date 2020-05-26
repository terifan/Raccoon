package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


final class HashTableInnerNode implements HashTableNode
{
	private HashTable mHashTable;
	private BlockPointer mBlockPointer;
	private HashTableInnerNode mParentNode;
	private NodeArray mChildNodes;


	public HashTableInnerNode(HashTable aHashTable, HashTableInnerNode aParent)
	{
		mHashTable = aHashTable;
		mParentNode = aParent;

		mChildNodes = new NodeArray(mHashTable, this, new byte[mHashTable.mNodeSize]);
	}


	public HashTableInnerNode(HashTable aHashTable, HashTableInnerNode aParentNode, BlockPointer aBlockPointer)
	{
		assert aHashTable.mPerformanceTool.tick("readNode");
		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mBlockPointer = aBlockPointer;

		mChildNodes = new NodeArray(mHashTable, this, mHashTable.readBlock(mBlockPointer));

		mHashTable.mCost.mReadBlockNode++;
	}


	void addNode(int aIndex, HashTableNode aNode, int aRange)
	{
		mChildNodes.set(aIndex, aNode, aRange);
	}


	@Override
	public BlockPointer getBlockPointer()
	{
		return mBlockPointer;
	}


	@Override
	public byte[] array()
	{
		return mChildNodes.array();
	}


	@Override
	public BlockType getType()
	{
		return BlockType.INDEX;
	}


	int findPointer(int aIndex)
	{
		for (; mChildNodes.isFree(aIndex); aIndex--)
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


	@Override
	public String integrityCheck()
	{
		int rangeRemain = 0;

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			BlockPointer bp = mChildNodes.getPointer2(i);

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
	public boolean get(ArrayMapEntry aEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("getValue");

		Log.i("get %s value", mHashTable.mTableName);

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = mChildNodes.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			case LEAF:
				HashTableNode node = mChildNodes.getNode(index);
				return node.get(aEntry, aHash, aLevel + 1);
			case HOLE:
				return false;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}
	}


	@Override
	public boolean put(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("putValue");

		Log.d("put %s value", mHashTable.mTableName);
		Log.inc();

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = mChildNodes.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableInnerNode node = mChildNodes.getNode(index);
				node.put(aEntry, oOldEntry, aHash, aLevel + 1);
				mChildNodes.markDirty(index, node, blockPointer.getRange());
				break;
			case LEAF:
				HashTableLeafNode leaf = mChildNodes.getNode(index);
				leaf.putValueLeaf(this, index, aEntry, oOldEntry, aHash, aLevel);
				break;
			case HOLE:
				HashTableLeafNode hole = mChildNodes.getNode(index);
				hole.upgradeHole(this, index, aEntry, aLevel, blockPointer.getRange());
				break;
			case FREE:
			default:
				throw new IllegalStateException("Block structure appears damaged, attempting to travese a free block");
		}

		Log.dec();

		return true;
	}


	@Override
	public boolean remove(ArrayMapEntry aEntry, Result<ArrayMapEntry> oOldEntry, long aHash, int aLevel)
	{
		assert mHashTable.mPerformanceTool.tick("removeValue");

		mHashTable.mCost.mTreeTraversal++;

		int index = findPointer(mHashTable.computeIndex(aHash, aLevel));
		BlockPointer blockPointer = mChildNodes.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTableInnerNode node = mChildNodes.getNode(index);
				if (node.remove(aEntry, oOldEntry, aHash, aLevel + 1))
				{
					mChildNodes.markDirty(index, node, blockPointer.getRange());
					return true;
				}
				return false;
			case LEAF:
				HashTableLeafNode leaf = mChildNodes.getNode(index);
				boolean found = leaf.remove(aEntry, oOldEntry, aHash, aLevel);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;
					mChildNodes.markDirty(index, leaf, blockPointer.getRange());
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
			HashTableNode node = mChildNodes.getNode(i);

			if (node != null)
			{
				node.visit(aVisitor);
			}
		}

		aVisitor.visit(this);
	}


	@Override
	public void scan(ScanResult aScanResult)
	{
		aScanResult.enterInnerNode(mBlockPointer);
		aScanResult.innerNodes++;

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTableNode node = mChildNodes.getNode(i);

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
						next = mChildNodes.getNode(index++);
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


	@Override
	public void clear()
	{
		Log.i("clear node %s", mBlockPointer);
		Log.inc();

		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTableNode node = mChildNodes.getNode(i);

			if (node != null)
			{
				node.clear();

				mChildNodes.freePointer(i);
			}
		}

		if (mBlockPointer != null)
		{
			mHashTable.freeBlock(mBlockPointer);
		}

		Log.dec();
	}


	void setNode(int aIndex, HashTableNode aNode, int aRange)
	{
		mHashTable.writeBlock(aNode, aRange);
		mChildNodes.set(aIndex, aNode, aRange);
	}
}
