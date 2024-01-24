package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.BlockType;
import java.util.Map.Entry;
import org.terifan.logging.Logger;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import static org.terifan.raccoon.BTree.CONF;
import static org.terifan.raccoon.BTree.INT_BLOCK_SIZE;
import static org.terifan.raccoon.BTree.LEAF_BLOCK_SIZE;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.util.Result;


public class BTreeInteriorNode extends BTreeNode
{
	NodeBuffer mChildNodes;


	BTreeInteriorNode(int aLevel)
	{
		super(aLevel);

		mChildNodes = new NodeBuffer();
	}


	@Override
	boolean get(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);

		mMap.loadNearestEntry(entry);

		BTreeNode node = getNode(aImplementation, entry);

		return node.get(aImplementation, aKey, aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		mMap.loadNearestEntry(nearestEntry);
		BTreeNode nearestNode = getNode(aImplementation, nearestEntry);

		int leafBlockSize = aImplementation.getConfiguration().getArray(CONF).getInt(LEAF_BLOCK_SIZE);
		int intBlockSize = aImplementation.getConfiguration().getArray(CONF).getInt(INT_BLOCK_SIZE);

		if (mLevel == 1 ? nearestNode.mMap.getCapacity() > leafBlockSize || nearestNode.mMap.getFreeSpace() < aEntry.getMarshalledLength() : nearestNode.mMap.getUsedSpace() > intBlockSize)
		{
			ArrayMapKey leftKey = nearestEntry.getKey();

			mChildNodes.remove(leftKey);
			mMap.remove(leftKey, null);

			SplitResult split = nearestNode.split(aImplementation);

			ArrayMapKey rightKey = split.getRightKey();

			mChildNodes.put(leftKey, split.getLeftNode());
			mChildNodes.put(rightKey, split.getRightNode());

			mMap.insert(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			mMap.insert(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

			nearestEntry = new ArrayMapEntry(aKey);
			mMap.loadNearestEntry(nearestEntry);
			nearestNode = getNode(aImplementation, nearestEntry);
		}

		return nearestNode.put(aImplementation, aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int offset = mMap.nearestIndex(aKey);

		BTreeNode curntChld = getNode(aImplementation, offset);
		BTreeNode leftChild = offset == 0 ? null : getNode(aImplementation, offset - 1);
		BTreeNode rghtChild = offset + 1 == mMap.size() ? null : getNode(aImplementation, offset + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? aImplementation.getConfiguration().getArray(CONF).getInt(LEAF_BLOCK_SIZE) : aImplementation.getConfiguration().getArray(CONF).getInt(INT_BLOCK_SIZE);

		if (leftChild != null && (curntChld.mMap.size() + leftChild.mMap.size()) < sizeLimit || rghtChild != null && (curntChld.mMap.size() + rghtChild.mMap.size()) < sizeLimit)
		{
			offset = mergeNodes(aImplementation, offset, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aImplementation, aKey, aOldEntry);

		if (curntChld.mLevel == 0 && curntChld.mMap.size() == 0)
		{
			if (offset == 0)
			{
				mMap.remove(ArrayMapKey.EMPTY, null);
				mChildNodes.remove(ArrayMapKey.EMPTY);

				ArrayMapEntry firstEntry = mMap.get(0, new ArrayMapEntry());

				BTreeNode firstNode = getNode(aImplementation, firstEntry);

				mMap.remove(firstEntry.getKey(), null);
				mChildNodes.remove(firstEntry.getKey());

				firstEntry.setKey(ArrayMapKey.EMPTY);

				mMap.insert(firstEntry);
				mChildNodes.put(firstEntry.getKey(), firstNode);
			}
			else
			{
				ArrayMapEntry targetEntry = mMap.get(offset, new ArrayMapEntry());

				mMap.remove(targetEntry.getKey(), null);
				mChildNodes.remove(targetEntry.getKey());
			}

			aImplementation.freeBlock(curntChld.mBlockPointer);
		}

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mMap.get(0, new ArrayMapEntry()).getKey().size() == 0 : "First key expected to be empty: " + mMap.toString();

		return result;
	}


	@Override
	void visit(BTree aImplementation, BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(aImplementation, this))
		{
			if (aVisitor.beforeInteriorNode(aImplementation, this, aLowestKey, aHighestKey))
			{
				mHighlight = BTree.RECORD_USE;

				for (int i = 0; i < mMap.size(); i++)
				{
					BTreeNode node = getNode(aImplementation, i);

					if (i == mMap.size() - 1)
					{
						node.visit(aImplementation, aVisitor, aLowestKey, aHighestKey);
					}
					else
					{
						ArrayMapKey nextHigh = getNode(aImplementation, i + 1).mMap.getKey(1);

						node.visit(aImplementation, aVisitor, aLowestKey, nextHigh);

						aLowestKey = node.mMap.getLast().getKey();
					}
				}
			}

			aVisitor.afterInteriorNode(aImplementation, this);
		}
	}


	private int mergeNodes(BTree aImplementation, int aOffset, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
		int newOffset = aOffset;

		boolean a = aLeftChild != null;
		if (a)
		{
			a &= aLeftChild.mMap.size() <= aKeyLimit || aCurntChld.mMap.size() <= aKeyLimit || aCurntChld.mMap.getUsedSpace() + aLeftChild.mMap.getUsedSpace() <= aSizeLimit;
		}

		boolean b = aRghtChild != null;
		if (b)
		{
			b &= aRghtChild.mMap.size() <= aKeyLimit || aCurntChld.mMap.size() <= aKeyLimit || aCurntChld.mMap.getUsedSpace() + aRghtChild.mMap.getUsedSpace() <= aSizeLimit;
		}

		if (a && b)
		{
			if (aLeftChild.mMap.getFreeSpace() < aRghtChild.mMap.getFreeSpace())
			{
				a = false;
			}
			else
			{
				b = false;
			}
		}

		if (mLevel == 1)
		{
			if (a)
			{
				mergeLeafs(aImplementation, newOffset - 1, (BTreeLeafNode)aLeftChild, (BTreeLeafNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeLeafs(aImplementation, newOffset + 1, (BTreeLeafNode)aRghtChild, (BTreeLeafNode)aCurntChld);
			}
			if (newOffset <= 0)
			{
				clearFirstKey();
			}
		}
		else
		{
			if (a)
			{
				mergeIndices(aImplementation, newOffset - 1, (BTreeInteriorNode)aLeftChild, (BTreeInteriorNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeIndices(aImplementation, newOffset + 1, (BTreeInteriorNode)aRghtChild, (BTreeInteriorNode)aCurntChld);
			}
		}

		// update lowest key after a merge
		if (newOffset > 0)
		{
			ArrayMapEntry nearestEntry = mMap.get(newOffset, new ArrayMapEntry());
			ArrayMapKey nearestKey = nearestEntry.getKey();

			ArrayMapKey firstKey = findLowestLeafKey(aImplementation, aCurntChld);

			if (!firstKey.equals(nearestEntry.getKey()))
			{
				nearestEntry.setKey(firstKey);

				mMap.remove(newOffset, null);
				mMap.insert(nearestEntry);

				BTreeNode childNode = mChildNodes.remove(nearestKey);
				if (childNode != null)
				{
					mChildNodes.put(firstKey, childNode);
				}
			}
		}

		return newOffset;
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getArray(CONF).getInt(INT_BLOCK_SIZE));

		BTreeInteriorNode left = new BTreeInteriorNode(mLevel);
		left.mMap = maps[0];
		left.mModified = true;

		BTreeInteriorNode right = new BTreeInteriorNode(mLevel);
		right.mMap = maps[1];
		right.mModified = true;

		ArrayMapKey midKey = right.mMap.getKey(0);

		for (Entry<ArrayMapKey, BTreeNode> childEntry : mChildNodes.entrySet())
		{
			if (childEntry.getKey().compareTo(midKey) < 0)
			{
				left.mChildNodes.put(childEntry.getKey(), childEntry.getValue());
			}
			else
			{
				right.mChildNodes.put(childEntry.getKey(), childEntry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
		ArrayMapKey keyRight = firstRight.getKey();

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mChildNodes.remove(keyRight);

		firstRight.setKey(keyLeft);

		right.mMap.insert(firstRight);
		right.mChildNodes.put(keyLeft, firstChild);

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow(BTree aImplementation)
	{
		SplitResult split = split(aImplementation);

		BTreeInteriorNode interior = new BTreeInteriorNode(mLevel + 1);
		interior.mModified = true;
		interior.mMap = new ArrayMap(aImplementation.getConfiguration().getArray(CONF).getInt(INT_BLOCK_SIZE));
		interior.mMap.insert(new ArrayMapEntry(split.getLeftKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		interior.mMap.insert(new ArrayMapEntry(split.getRightKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		interior.mChildNodes.put(split.getLeftKey(), split.getLeftNode());
		interior.mChildNodes.put(split.getRightKey(), split.getRightNode());

		mChildNodes.clear();

		return interior;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeInteriorNode shrink(BTree aImplementation)
	{
		BTreeInteriorNode interior = new BTreeInteriorNode(mLevel - 1);
		interior.mModified = true;
		interior.mMap = new ArrayMap(aImplementation.getConfiguration().getArray(CONF).getInt(INT_BLOCK_SIZE));

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeInteriorNode node = getNode(aImplementation, i);

			boolean first = i > 0;
			for (ArrayMapEntry entry : node.mMap)
			{
				ArrayMapEntry newEntry;
				if (first)
				{
					newEntry = new ArrayMapEntry(mMap.getKey(i), entry);
					first = false;
				}
				else
				{
					newEntry = entry;
				}

				interior.mMap.insert(newEntry);

				BTreeNode childNode = node.mChildNodes.remove(entry.getKey());
				if (childNode != null)
				{
					interior.mChildNodes.put(newEntry.getKey(), childNode);
				}
			}

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return interior;
	}


	private void mergeIndices(BTree aImplementation, int aFromIndex, BTreeInteriorNode aFrom, BTreeInteriorNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(aImplementation, aFrom);
		fixFirstKey(aImplementation, aTo);

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			BTreeNode node = aFrom.getNode(aImplementation, temp);

			aTo.mMap.insert(temp);
			aTo.mChildNodes.put(temp.getKey(), node);
		}

		aFrom.mMap.clear();
		aFrom.mChildNodes.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		aTo.clearFirstKey();

		mMap.get(aFromIndex, temp);
		mMap.remove(aFromIndex, null);
		mChildNodes.remove(temp.getKey());

		if (aFromIndex == 0)
		{
			clearFirstKey();
		}

		aTo.mModified = true;
	}


	private void mergeLeafs(BTree aImplementation, int aOffset, BTreeLeafNode aFrom, BTreeLeafNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp);
		}

		aFrom.mMap.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aOffset, temp);
		mMap.remove(aOffset, null);

		mChildNodes.remove(temp.getKey());

		aTo.mModified = true;
	}


	private void fixFirstKey(BTree aImplementation, BTreeInteriorNode aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.get(0, new ArrayMapEntry());

		assert firstEntry.getKey().size() == 0;

		BTreeNode firstNode = aNode.getNode(aImplementation, firstEntry);

		firstEntry.setKey(findLowestLeafKey(aImplementation, aNode));

		aNode.mMap.remove(ArrayMapKey.EMPTY, null);
		aNode.mChildNodes.remove(ArrayMapKey.EMPTY);

		aNode.mMap.insert(firstEntry);
		aNode.mChildNodes.put(firstEntry.getKey(), firstNode);
	}


	private void clearFirstKey()
	{
		ArrayMapEntry firstEntry = mMap.get(0, new ArrayMapEntry());

		if (firstEntry.getKey().size() > 0)
		{
			mMap.removeFirst();

			BTreeNode childNode = mChildNodes.remove(firstEntry.getKey());

			firstEntry.setKey(ArrayMapKey.EMPTY);

			mMap.insert(firstEntry);

			if (childNode != null)
			{
				mChildNodes.put(ArrayMapKey.EMPTY, childNode);
			}
		}
	}


	private static ArrayMapKey findLowestLeafKey(BTree aImplementation, BTreeNode aNode)
	{
		if (aNode instanceof BTreeInteriorNode)
		{
			BTreeInteriorNode node = (BTreeInteriorNode)aNode;
			for (int i = 0; i < node.mMap.size(); i++)
			{
				ArrayMapKey b = findLowestLeafKey(aImplementation, node.getNode(aImplementation, i));
				if (b.size() > 0)
				{
					return b;
				}
			}
			throw new IllegalStateException();
		}

//		assert !aNode.mMap.isEmpty();
		if (aNode.mMap.isEmpty())
		{
			return ArrayMapKey.EMPTY;
		}

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeafNode downgrade(BTree aImplementation)
	{
		assert mLevel == 1;

		BTreeLeafNode newLeaf = getNode(aImplementation, 0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeafNode node = getNode(aImplementation, i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e));

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit(BTree aImplementation)
	{
		assert assertValidCache() == null : assertValidCache();

		for (Entry<ArrayMapKey, BTreeNode> entry : mChildNodes.entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit(aImplementation))
			{
				mModified = true;

				mMap.insert(new ArrayMapEntry(entry.getKey(), node.mBlockPointer, TYPE_TREENODE));
			}
		}

		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(mMap.array(), mLevel, BlockType.BTREE_NODE);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		if (mModified)
		{
			for (BTreeNode node : mChildNodes.values())
			{
				node.postCommit();
			}

			mModified = false;
		}

		mChildNodes.clear();
	}


	int size()
	{
		return mMap.size();
	}


	<T extends BTreeNode> T getNode(BTree aImplementation, int aOffset)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aOffset, entry);

		BTreeNode node = getNode(aImplementation, entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(BTree aImplementation, ArrayMapEntry aEntry)
	{
		ArrayMapKey key = aEntry.getKey();

		BTreeNode childNode = mChildNodes.get(key);

		if (childNode == null)
		{
			BlockPointer bp = aEntry.getBlockPointer();

			childNode = bp.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(mLevel - 1) : new BTreeLeafNode();
			childNode.mBlockPointer = bp;
			childNode.mMap = new ArrayMap(aImplementation.readBlock(bp));

			mChildNodes.put(key, childNode);

			RuntimeDiagnostics.collectStatistics(bp.getBlockType() == BlockType.BTREE_NODE ? Operation.READ_NODE : Operation.READ_LEAF, 1);
		}

		return childNode;
	}


	@Override
	public String toString()
	{
		String s = String.format("BTreeIndex{mLevel={}, mMap=" + mMap + ", mBuffer={", mLevel);
		for (ArrayMapKey t : mChildNodes.keySet())
		{
			s += String.format("\"%s\",", t);
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	private String assertValidCache()
	{
		for (ArrayMapKey key : mChildNodes.keySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			if (!mMap.get(entry))
			{
				return entry.toString();
			}
		}
		return null;
	}
}
