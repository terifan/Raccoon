package org.terifan.raccoon;

import java.util.Iterator;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


class HashTreeTableInnerNode implements HashTreeTableNode
{
	private HashTreeTable mHashTable;
	private BlockPointer mBlockPointer;
	private HashTreeTableInnerNode mParentNode;
	private HashTreeTableNodeArray mNodesArray;


	public HashTreeTableInnerNode(HashTreeTable aHashTable, HashTreeTableInnerNode aParent)
	{
		mHashTable = aHashTable;
		mParentNode = aParent;

		mNodesArray = new HashTreeTableNodeArray(mHashTable, this, new byte[mHashTable.mNodeSize]);
	}


	public HashTreeTableInnerNode(HashTreeTable aHashTable, HashTreeTableInnerNode aParentNode, BlockPointer aBlockPointer)
	{
		assert aHashTable.mPerformanceTool.tick("readNode");
		assert aBlockPointer.getBlockType() == BlockType.INDEX;

		mHashTable = aHashTable;
		mParentNode = aParentNode;
		mBlockPointer = aBlockPointer;

		mNodesArray = new HashTreeTableNodeArray(mHashTable, this, mHashTable.readBlock(mBlockPointer));

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
		return mNodesArray.array();
	}


	@Override
	public BlockType getType()
	{
		return BlockType.INDEX;
	}


	private int findPointer(int aIndex)
	{
		for (; mNodesArray.isFree(aIndex); aIndex--)
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
			BlockPointer bp = mNodesArray.getPointer2(i);

			if (rangeRemain > 0)
			{
				if (bp.getUserData() != 0)
				{
					return "Pointer inside range";
				}
			}
			else
			{
				if (bp.getUserData() == 0)
				{
					return "Zero range";
				}
				rangeRemain = (int)bp.getUserData();
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
		BlockPointer blockPointer = mNodesArray.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
			case LEAF:
				HashTreeTableNode node = mNodesArray.getNode(index);
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
		BlockPointer blockPointer = mNodesArray.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTreeTableInnerNode node = mNodesArray.getNode(index);
				node.put(aEntry, oOldEntry, aHash, aLevel + 1);
				mNodesArray.markDirty(index, node, (int)blockPointer.getUserData());
				break;
			case LEAF:
				HashTreeTableLeafNode leaf = mNodesArray.getNode(index);
				leaf.putValueLeaf(index, aEntry, oOldEntry, aHash, aLevel);
				break;
			case HOLE:
				HashTreeTableLeafNode hole = mNodesArray.getNode(index);
				hole.upgradeHole(index, aEntry, aLevel, (int)blockPointer.getUserData());
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
		BlockPointer blockPointer = mNodesArray.getPointer(index);

		switch (blockPointer.getBlockType())
		{
			case INDEX:
				HashTreeTableInnerNode node = mNodesArray.getNode(index);
				if (node.remove(aEntry, oOldEntry, aHash, aLevel + 1))
				{
					mNodesArray.markDirty(index, node, (int)blockPointer.getUserData());
					return true;
				}
				return false;
			case LEAF:
				HashTreeTableLeafNode leaf = mNodesArray.getNode(index);
				boolean found = leaf.remove(aEntry, oOldEntry, aHash, aLevel);

				if (found)
				{
					mHashTable.mCost.mEntityRemove++;
					mNodesArray.markDirty(index, leaf, (int)blockPointer.getUserData());
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
	public void visit(HashTreeTableVisitor aVisitor)
	{
		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTreeTableNode node = mNodesArray.getNode(i);

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
			HashTreeTableNode node = mNodesArray.getNode(i);

			if (node != null)
			{
				node.scan(aScanResult);
			}
		}

		aScanResult.exitInnerNode();
	}


	public Iterator<HashTreeTableNode> iterator()
	{
		return new Iterator<HashTreeTableNode>()
		{
			int index;
			HashTreeTableNode next;


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
						next = mNodesArray.getNode(index++);
					}
				}

				return next != null;
			}


			@Override
			public HashTreeTableNode next()
			{
				if (next == null)
				{
					throw new IllegalStateException();
				}
				HashTreeTableNode tmp = next;
				next = null;
				return tmp;
			}
		};
	}


	@Override
	public BlockPointer flush()
	{
		freeNode(this);

		mBlockPointer = mHashTable.writeBlock(this, mHashTable.mPointersPerNode);

		return mBlockPointer;
	}


	@Override
	public void clear()
	{
		for (int i = 0; i < mHashTable.mPointersPerNode; i++)
		{
			HashTreeTableNode node = mNodesArray.getNode(i);

			if (node != null)
			{
				node.clear();

				mNodesArray.freePointer(i);
			}
		}

		freeNode(this);
	}


	public void setNode(int aIndex, HashTreeTableNode aNode, int aRange)
	{
		mNodesArray.set(aIndex, aNode, aRange);
	}


	public void freeNode(HashTreeTableNode aNode)
	{
		BlockPointer bp = aNode.getBlockPointer();

		if (bp != null && bp.getBlockType() != BlockType.HOLE && bp.getBlockType() != BlockType.FREE)
		{
			mHashTable.freeBlock(bp);
		}
	}
}
