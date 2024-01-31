package org.terifan.raccoon;

import java.util.Map.Entry;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import static org.terifan.raccoon.BTree.CONF;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.util.Result;
import org.terifan.raccoon.blockdevice.BlockType;
import static org.terifan.raccoon.BTree.NODE_SIZE;
import static org.terifan.raccoon.BTree.LEAF_SIZE;


public class BTreeInteriorNode extends BTreeNode
{
	NodeBuffer mChildNodes;


	BTreeInteriorNode(int aLevel, ArrayMap aMap)
	{
		super(aLevel);

		mChildNodes = new NodeBuffer(aMap);
	}


	@Override
	boolean get(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);

		mChildNodes.loadNearestEntry(entry);

		BTreeNode node = getNode(aImplementation, entry);

		return node.get(aImplementation, aKey, aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		mChildNodes.loadNearestEntry(nearestEntry);
		BTreeNode nearestNode = getNode(aImplementation, nearestEntry);

		int leafBlockSize = aImplementation.getConfiguration().getArray(CONF).getInt(LEAF_SIZE);
		int intBlockSize = aImplementation.getConfiguration().getArray(CONF).getInt(NODE_SIZE);

		if (mLevel == 1 ? ((BTreeLeafNode)nearestNode).mMap.getCapacity() > leafBlockSize || ((BTreeLeafNode)nearestNode).mMap.getFreeSpace() < aEntry.getMarshalledLength() : ((BTreeLeafNode)nearestNode).mMap.getUsedSpace() > intBlockSize)
		{
			ArrayMapKey leftKey = nearestEntry.getKey();

			mChildNodes.remove(leftKey);
			mChildNodes.removeEntry(leftKey, null);

			SplitResult split = nearestNode.split(aImplementation);

			ArrayMapKey rightKey = split.getRightKey();

			mChildNodes.put(leftKey, split.getLeftNode());
			mChildNodes.put(rightKey, split.getRightNode());

			mChildNodes.insertEntry(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			mChildNodes.insertEntry(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

			nearestEntry = new ArrayMapEntry(aKey);
			mChildNodes.loadNearestEntry(nearestEntry);
			nearestNode = getNode(aImplementation, nearestEntry);

			assert childCount() >= 2;
		}

		return nearestNode.put(aImplementation, aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int offset = mChildNodes.nearestIndex(aKey);

		BTreeNode curntChld = getNode(aImplementation, offset);
		BTreeNode leftChild = offset == 0 ? null : getNode(aImplementation, offset - 1);
		BTreeNode rghtChild = offset + 1 == childCount() ? null : getNode(aImplementation, offset + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? aImplementation.getConfiguration().getArray(CONF).getInt(LEAF_SIZE) : aImplementation.getConfiguration().getArray(CONF).getInt(NODE_SIZE);

		if (leftChild != null && (curntChld.childCount() + leftChild.childCount()) < sizeLimit || rghtChild != null && (curntChld.childCount() + rghtChild.childCount()) < sizeLimit)
		{
			offset = mergeNodes(aImplementation, offset, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aImplementation, aKey, aOldEntry);

		if (curntChld.mLevel == 0 && ((BTreeLeafNode)curntChld).mMap.size() == 0)
		{
			if (offset == 0)
			{
				mChildNodes.removeEntry(ArrayMapKey.EMPTY, null);
				mChildNodes.remove(ArrayMapKey.EMPTY);

				ArrayMapEntry firstEntry = mChildNodes.getEntry(0, new ArrayMapEntry());

				BTreeNode firstNode = getNode(aImplementation, firstEntry);

				mChildNodes.removeEntry(firstEntry.getKey(), null);
				mChildNodes.remove(firstEntry.getKey());

				firstEntry.setKey(ArrayMapKey.EMPTY);

				mChildNodes.insertEntry(firstEntry);
				mChildNodes.put(firstEntry.getKey(), firstNode);
			}
			else
			{
				ArrayMapEntry targetEntry = mChildNodes.getEntry(offset, new ArrayMapEntry());

				mChildNodes.removeEntry(targetEntry.getKey(), null);
				mChildNodes.remove(targetEntry.getKey());
			}

			aImplementation.freeBlock(curntChld.mBlockPointer);
		}

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mChildNodes.getEntry(0, new ArrayMapEntry()).getKey().size() == 0 : "First key expected to be empty: " + mChildNodes.toString();
		assert childCount() >= 2;

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

				ArrayMapKey lowestKey = aLowestKey;
				BTreeNode node = getNode(aImplementation, 0);

				for (int i = 1, n = childCount() - 1; i < n; i++)
				{
					BTreeNode nextNode = getNode(aImplementation, i);

					if (nextNode.childCount() == 1)
					{
						System.out.println("-".repeat(200));
						System.out.println(nextNode.mBlockPointer==null?"null":nextNode.mBlockPointer.marshalDocument());
						System.out.println(nextNode.mHighlight);
						System.out.println(nextNode.mLevel);
						System.out.println(nextNode);
						System.out.println(nextNode.mModified);
						System.out.println("-".repeat(200));
					}

					if (nextNode instanceof BTreeInteriorNode)
					{
						ArrayMapKey nextHigh = nextNode.childCount() == 1 ? aHighestKey : ((BTreeInteriorNode)nextNode).mChildNodes.getKey(1);
						node.visit(aImplementation, aVisitor, lowestKey, nextHigh);
						lowestKey = ((BTreeInteriorNode)node).mChildNodes.getLast().getKey();
					}
					else
					{
						ArrayMapKey nextHigh = nextNode.childCount() == 1 ? aHighestKey : ((BTreeLeafNode)nextNode).mMap.getKey(1);
						node.visit(aImplementation, aVisitor, lowestKey, nextHigh);
						lowestKey = ((BTreeLeafNode)node).mMap.getLast().getKey();
					}

					node = nextNode;
				}

				node.visit(aImplementation, aVisitor, lowestKey, aHighestKey);
			}

			aVisitor.afterInteriorNode(aImplementation, this);
		}
	}


	private int mergeNodes(BTree aImplementation, int aOffset, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
		int newOffset = aOffset;

		boolean a = aLeftChild != null;
		boolean b = aRghtChild != null;

		if (aLeftChild instanceof BTreeInteriorNode)
		{
			if (a)
			{
				a &= aLeftChild.childCount() <= aKeyLimit || aCurntChld.childCount() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).mChildNodes.getUsedSpace() + ((BTreeInteriorNode)aLeftChild).mChildNodes.getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.childCount() <= aKeyLimit || aCurntChld.childCount() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).mChildNodes.getUsedSpace() + ((BTreeInteriorNode)aRghtChild).mChildNodes.getUsedSpace() <= aSizeLimit;
			}

			if (a && b)
			{
				if (((BTreeInteriorNode)aLeftChild).mChildNodes.getFreeSpace() < ((BTreeInteriorNode)aRghtChild).mChildNodes.getFreeSpace())
				{
					a = false;
				}
				else
				{
					b = false;
				}
			}
		}
		else
		{
			if (a)
			{
				a &= aLeftChild.childCount() <= aKeyLimit || aCurntChld.childCount() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aLeftChild).mMap.getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.childCount() <= aKeyLimit || aCurntChld.childCount() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aRghtChild).mMap.getUsedSpace() <= aSizeLimit;
			}

			if (a && b)
			{
				if (((BTreeLeafNode)aLeftChild).mMap.getFreeSpace() < ((BTreeLeafNode)aRghtChild).mMap.getFreeSpace())
				{
					a = false;
				}
				else
				{
					b = false;
				}
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
			ArrayMapEntry nearestEntry = mChildNodes.getEntry(newOffset, new ArrayMapEntry());
			ArrayMapKey nearestKey = nearestEntry.getKey();

			ArrayMapKey firstKey = findLowestLeafKey(aImplementation, aCurntChld);

			if (!firstKey.equals(nearestEntry.getKey()))
			{
				nearestEntry.setKey(firstKey);

				mChildNodes.removeEntry(newOffset, null);
				mChildNodes.insertEntry(nearestEntry);

				BTreeNode childNode = mChildNodes.remove(nearestKey);
				if (childNode != null)
				{
					mChildNodes.put(firstKey, childNode);
				}
			}

			assert childCount() >= 2;
		}

		return newOffset;
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mChildNodes.split(aImplementation.getConfiguration().getArray(CONF).getInt(NODE_SIZE));

		BTreeInteriorNode left = new BTreeInteriorNode(mLevel, maps[0]);
		left.mModified = true;

		BTreeInteriorNode right = new BTreeInteriorNode(mLevel, maps[1]);
		right.mModified = true;

		ArrayMapKey midKey = right.mChildNodes.getKey(0);

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

		ArrayMapEntry firstRight = right.mChildNodes.getFirst();

		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
		ArrayMapKey keyRight = firstRight.getKey();

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mChildNodes.removeEntry(firstRight.getKey(), null);
		right.mChildNodes.remove(keyRight);

		firstRight.setKey(keyLeft);

		right.mChildNodes.insertEntry(firstRight);
		right.mChildNodes.put(keyLeft, firstChild);

		assert left.childCount() >= 2;
		assert right.childCount() >= 2;

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow(BTree aImplementation)
	{
		SplitResult split = split(aImplementation);

		BTreeInteriorNode interior = new BTreeInteriorNode(mLevel + 1, new ArrayMap(aImplementation.getConfiguration().getArray(CONF).getInt(NODE_SIZE)));
		interior.mModified = true;
		interior.mChildNodes.insertEntry(new ArrayMapEntry(split.getLeftKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		interior.mChildNodes.insertEntry(new ArrayMapEntry(split.getRightKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
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
		BTreeInteriorNode interior = new BTreeInteriorNode(mLevel - 1, new ArrayMap(aImplementation.getConfiguration().getArray(CONF).getInt(NODE_SIZE)));
		interior.mModified = true;

		for (int i = 0; i < childCount(); i++)
		{
			BTreeInteriorNode node = getNode(aImplementation, i);

			boolean first = i > 0;
			for (ArrayMapEntry entry : node.mChildNodes)
			{
				ArrayMapEntry newEntry;
				if (first)
				{
					newEntry = new ArrayMapEntry(mChildNodes.getKey(i), entry);
					first = false;
				}
				else
				{
					newEntry = entry;
				}

				interior.mChildNodes.insertEntry(newEntry);

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

		for (int i = 0, sz = aFrom.childCount(); i < sz; i++)
		{
			aFrom.mChildNodes.getEntry(i, temp);

			BTreeNode node = aFrom.getNode(aImplementation, temp);

			aTo.mChildNodes.insertEntry(temp);
			aTo.mChildNodes.put(temp.getKey(), node);
		}

		aFrom.mChildNodes.clearEntries();
		aFrom.mChildNodes.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		aTo.clearFirstKey();

		mChildNodes.getEntry(aFromIndex, temp);
		mChildNodes.removeEntry(aFromIndex, null);
		mChildNodes.remove(temp.getKey());

		if (aFromIndex == 0)
		{
			clearFirstKey();
		}

		aTo.mModified = true;

		assert childCount() >= 2;
		assert aFrom.childCount() >= 2;
		assert aTo.childCount() >= 2;
	}


	private void mergeLeafs(BTree aImplementation, int aOffset, BTreeLeafNode aFrom, BTreeLeafNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.childCount(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);
			aTo.mMap.insert(temp);
		}

		aFrom.mMap.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		mChildNodes.getEntry(aOffset, temp);
		mChildNodes.removeEntry(aOffset, null);

		mChildNodes.remove(temp.getKey());

		aTo.mModified = true;
	}


	private void fixFirstKey(BTree aImplementation, BTreeInteriorNode aNode)
	{
		ArrayMapEntry firstEntry = aNode.mChildNodes.getEntry(0, new ArrayMapEntry());

		assert firstEntry.getKey().size() == 0;

		BTreeNode firstNode = aNode.getNode(aImplementation, firstEntry);

		firstEntry.setKey(findLowestLeafKey(aImplementation, aNode));

		aNode.mChildNodes.removeEntry(ArrayMapKey.EMPTY, null);
		aNode.mChildNodes.remove(ArrayMapKey.EMPTY);

		aNode.mChildNodes.insertEntry(firstEntry);
		aNode.mChildNodes.put(firstEntry.getKey(), firstNode);
	}


	private void clearFirstKey()
	{
		ArrayMapEntry firstEntry = mChildNodes.getEntry(0, new ArrayMapEntry());

		if (firstEntry.getKey().size() > 0)
		{
			mChildNodes.removeFirst();

			BTreeNode childNode = mChildNodes.remove(firstEntry.getKey());

			firstEntry.setKey(ArrayMapKey.EMPTY);

			mChildNodes.insertEntry(firstEntry);

			if (childNode != null)
			{
				mChildNodes.put(ArrayMapKey.EMPTY, childNode);
			}
		}
	}


	private static ArrayMapKey findLowestLeafKey(BTree aImplementation, BTreeNode aNode)
	{
		if (aNode instanceof BTreeInteriorNode v)
		{
			for (int i = 0; i < v.childCount(); i++)
			{
				ArrayMapKey b = findLowestLeafKey(aImplementation, v.getNode(aImplementation, i));
				if (b.size() > 0)
				{
					return b;
				}
			}
			throw new IllegalStateException();
		}

		BTreeLeafNode leaf = (BTreeLeafNode)aNode;

		if (leaf.mMap.isEmpty())
		{
			return ArrayMapKey.EMPTY;
		}

		return leaf.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeafNode downgrade(BTree aImplementation)
	{
		assert mLevel == 1;

		BTreeLeafNode newLeaf = getNode(aImplementation, 0);
		newLeaf.mModified = true;

		for (int i = 1; i < childCount(); i++)
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
		assert childCount() >= 2;

		for (Entry<ArrayMapKey, BTreeNode> entry : mChildNodes.entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit(aImplementation))
			{
				mModified = true;

				mChildNodes.insertEntry(new ArrayMapEntry(entry.getKey(), node.mBlockPointer, TYPE_TREENODE));
			}
		}

		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(mChildNodes.array(), mLevel, BlockType.BTREE_NODE);
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
		return childCount();
	}


	<T extends BTreeNode> T getNode(BTree aImplementation, int aOffset)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mChildNodes.getEntry(aOffset, entry);

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

			childNode = bp.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(mLevel - 1, new ArrayMap(aImplementation.readBlock(bp))) : new BTreeLeafNode(new ArrayMap(aImplementation.readBlock(bp)));
			childNode.mBlockPointer = bp;

			mChildNodes.put(key, childNode);

			RuntimeDiagnostics.collectStatistics(bp.getBlockType() == BlockType.BTREE_NODE ? Operation.READ_NODE : Operation.READ_LEAF, 1);
		}

		return childNode;
	}


	@Override
	public String toString()
	{
		String s = String.format("BTreeInteriorNode{mLevel=%s, mMap=%s, mBuffer={", mLevel, mChildNodes);
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
			if (!mChildNodes.getEntry(entry))
			{
				return entry.toString();
			}
		}
		return null;
	}


	@Override
	protected String integrityCheck()
	{
		return mChildNodes.integrityCheck();
	}


	@Override
	protected int childCount()
	{
		return mChildNodes.size();
	}
}
