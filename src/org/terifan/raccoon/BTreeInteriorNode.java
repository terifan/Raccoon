package org.terifan.raccoon;

import java.util.Map.Entry;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.util.Result;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTreeInteriorNode extends BTreeNode
{
	NodeBuffer mChildNodes;


	BTreeInteriorNode(BTree aTree, int aLevel, ArrayMap aMap)
	{
		super(aTree, aLevel);

		mChildNodes = new NodeBuffer(aMap);
	}


	@Override
	boolean get(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);

		mChildNodes.loadNearestEntry(entry);

		BTreeNode node = getNode(entry);

		return node.get(aKey, aEntry);
	}


	@Override
	PutResult put(ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		mChildNodes.loadNearestEntry(nearestEntry);
		BTreeNode nearestNode = getNode(nearestEntry);

		if (mLevel == 1 ? ((BTreeLeafNode)nearestNode).mMap.getCapacity() > mTree.getLeafSize() || ((BTreeLeafNode)nearestNode).mMap.getFreeSpace() < aEntry.getMarshalledLength() : ((BTreeInteriorNode)nearestNode).mChildNodes.getUsedSpace() > mTree.getNodeSize())
		{
			ArrayMapKey leftKey = nearestEntry.getKey();

			mChildNodes.remove(leftKey);
			mChildNodes.removeEntry(leftKey, null);

			SplitResult split = nearestNode.split();

			ArrayMapKey rightKey = split.getRightKey();

			mChildNodes.put(leftKey, split.getLeftNode());
			mChildNodes.put(rightKey, split.getRightNode());

			mChildNodes.insertEntry(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			mChildNodes.insertEntry(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

			nearestEntry = new ArrayMapEntry(aKey);
			mChildNodes.loadNearestEntry(nearestEntry);
			nearestNode = getNode(nearestEntry);

			assert size() >= 2;
		}

		return nearestNode.put(aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int offset = mChildNodes.nearestIndex(aKey);

		BTreeNode curntChld = getNode(offset);
		BTreeNode leftChild = offset == 0 ? null : getNode(offset - 1);
		BTreeNode rghtChild = offset + 1 == size() ? null : getNode(offset + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? mTree.getLeafSize() : mTree.getNodeSize();

		if (leftChild != null && (curntChld.size() + leftChild.size()) < sizeLimit || rghtChild != null && (curntChld.size() + rghtChild.size()) < sizeLimit)
		{
			offset = mergeNodes(offset, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aKey, aOldEntry);

		if (curntChld.mLevel == 0 && ((BTreeLeafNode)curntChld).mMap.size() == 0)
		{
			if (offset == 0)
			{
				mChildNodes.removeEntry(ArrayMapKey.EMPTY, null);
				mChildNodes.remove(ArrayMapKey.EMPTY);

				ArrayMapEntry firstEntry = mChildNodes.getEntry(0, new ArrayMapEntry());

				BTreeNode firstNode = getNode(firstEntry);

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

			mTree.freeBlock(curntChld.mBlockPointer);
		}

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mChildNodes.getEntry(0, new ArrayMapEntry()).getKey().size() == 0 : "First key expected to be empty: " + mChildNodes.toString();
		assert size() >= 2;

		return result;
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeInteriorNode(this, aLowestKey, aHighestKey))
			{
				mHighlight = BTree.RECORD_USE;

				ArrayMapKey lowestKey = aLowestKey;
				BTreeNode node = getNode(0);

				for (int i = 1, n = size(); i < n; i++)
				{
					BTreeNode nextNode = getNode(i);

					if (nextNode instanceof BTreeInteriorNode)
					{
						if (nextNode.size() == 1)
						{
							System.out.println("-".repeat(200));
							System.out.println(nextNode.mBlockPointer==null?"null":nextNode.mBlockPointer.marshalDocument());
							System.out.println(nextNode.mHighlight);
							System.out.println(nextNode.mLevel);
							System.out.println(nextNode);
							System.out.println(nextNode.mModified);
							System.out.println("-".repeat(200));
						}

						ArrayMapKey nextHigh = nextNode.size() == 1 ? aHighestKey : ((BTreeInteriorNode)nextNode).mChildNodes.getKey(1);
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = ((BTreeInteriorNode)node).mChildNodes.getLast().getKey();
					}
					else
					{
						ArrayMapKey nextHigh = ((BTreeLeafNode)nextNode).mMap.getLast().getKey();
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = ((BTreeLeafNode)node).mMap.getLast().getKey();
					}

					node = nextNode;
				}

				node.visit(aVisitor, lowestKey, aHighestKey);
			}

			aVisitor.afterInteriorNode(this);
		}
	}


	private int mergeNodes(int aOffset, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
		int newOffset = aOffset;

		boolean a = aLeftChild != null;
		boolean b = aRghtChild != null;

		if (aLeftChild instanceof BTreeInteriorNode)
		{
			if (a)
			{
				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).mChildNodes.getUsedSpace() + ((BTreeInteriorNode)aLeftChild).mChildNodes.getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).mChildNodes.getUsedSpace() + ((BTreeInteriorNode)aRghtChild).mChildNodes.getUsedSpace() <= aSizeLimit;
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
				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aLeftChild).mMap.getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aRghtChild).mMap.getUsedSpace() <= aSizeLimit;
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
				mergeLeafs(newOffset - 1, (BTreeLeafNode)aLeftChild, (BTreeLeafNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeLeafs(newOffset + 1, (BTreeLeafNode)aRghtChild, (BTreeLeafNode)aCurntChld);
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
				mergeIndices(newOffset - 1, (BTreeInteriorNode)aLeftChild, (BTreeInteriorNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeIndices(newOffset + 1, (BTreeInteriorNode)aRghtChild, (BTreeInteriorNode)aCurntChld);
			}
		}

		// update lowest key after a merge
		if (newOffset > 0)
		{
			ArrayMapEntry nearestEntry = mChildNodes.getEntry(newOffset, new ArrayMapEntry());
			ArrayMapKey nearestKey = nearestEntry.getKey();

			ArrayMapKey firstKey = findLowestLeafKey(aCurntChld);

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

			assert size() >= 2;
		}

		return newOffset;
	}


	@Override
	SplitResult split()
	{
		mTree.freeBlock(mBlockPointer);

		ArrayMap[] maps = mChildNodes.split(mTree.getNodeSize());

		BTreeInteriorNode left = new BTreeInteriorNode(mTree, mLevel, maps[0]);
		BTreeInteriorNode right = new BTreeInteriorNode(mTree, mLevel, maps[1]);
		left.mModified = true;
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

		BTreeNode firstChild = right.getNode(firstRight);

		right.mChildNodes.removeEntry(firstRight.getKey(), null);
		right.mChildNodes.remove(keyRight);

		firstRight.setKey(keyLeft);

		right.mChildNodes.insertEntry(firstRight);
		right.mChildNodes.put(keyLeft, firstChild);

		assert left.size() >= 2;
		assert right.size() >= 2;

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow()
	{
		SplitResult split = split();

		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mLevel + 1, new ArrayMap(mTree.getNodeSize()));
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
	BTreeInteriorNode shrink()
	{
		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mLevel - 1, new ArrayMap(mTree.getNodeSize()));
		interior.mModified = true;

		for (int i = 0; i < size(); i++)
		{
			BTreeInteriorNode node = getNode(i);

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

			mTree.freeBlock(node.mBlockPointer);
		}

		mTree.freeBlock(mBlockPointer);

		return interior;
	}


	private void mergeIndices(int aFromIndex, BTreeInteriorNode aFrom, BTreeInteriorNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(aFrom);
		fixFirstKey(aTo);

		for (int i = 0, sz = aFrom.size(); i < sz; i++)
		{
			aFrom.mChildNodes.getEntry(i, temp);

			BTreeNode node = aFrom.getNode(temp);

			aTo.mChildNodes.insertEntry(temp);
			aTo.mChildNodes.put(temp.getKey(), node);
		}

		aFrom.mChildNodes.clearEntries();
		aFrom.mChildNodes.clear();
		mTree.freeBlock(aFrom.mBlockPointer);

		aTo.clearFirstKey();

		mChildNodes.getEntry(aFromIndex, temp);
		mChildNodes.removeEntry(aFromIndex, null);
		mChildNodes.remove(temp.getKey());

		if (aFromIndex == 0)
		{
			clearFirstKey();
		}

		aTo.mModified = true;

		assert size() >= 2;
		assert aFrom.size() >= 2;
		assert aTo.size() >= 2;
	}


	private void mergeLeafs(int aOffset, BTreeLeafNode aFrom, BTreeLeafNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);
			aTo.mMap.insert(temp);
		}

		aFrom.mMap.clear();
		mTree.freeBlock(aFrom.mBlockPointer);

		mChildNodes.getEntry(aOffset, temp);
		mChildNodes.removeEntry(aOffset, null);
		mChildNodes.remove(temp.getKey());

		aTo.mModified = true;
	}


	private void fixFirstKey(BTreeInteriorNode aNode)
	{
		ArrayMapEntry firstEntry = aNode.mChildNodes.getEntry(0, new ArrayMapEntry());

		assert firstEntry.getKey().size() == 0;

		BTreeNode firstNode = aNode.getNode(firstEntry);

		firstEntry.setKey(findLowestLeafKey(aNode));

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


	private static ArrayMapKey findLowestLeafKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeInteriorNode v)
		{
			for (int i = 0; i < v.size(); i++)
			{
				ArrayMapKey b = findLowestLeafKey(v.getNode(i));
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
	BTreeLeafNode downgrade()
	{
		assert mLevel == 1;

		BTreeLeafNode newLeaf = getNode(0);
		newLeaf.mModified = true;

		for (int i = 1; i < size(); i++)
		{
			BTreeLeafNode node = getNode(i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e));

			mTree.freeBlock(node.mBlockPointer);
		}

		mTree.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit()
	{
		assert assertValidCache() == null : assertValidCache();
		assert size() >= 2;

		for (Entry<ArrayMapKey, BTreeNode> entry : mChildNodes.entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit())
			{
				mModified = true;

				mChildNodes.insertEntry(new ArrayMapEntry(entry.getKey(), node.mBlockPointer, TYPE_TREENODE));
			}
		}

		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			mTree.freeBlock(mBlockPointer);

			mBlockPointer = mTree.writeBlock(mChildNodes.array(), mLevel, BlockType.BTREE_NODE);
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


	<T extends BTreeNode> T getNode(int aOffset)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mChildNodes.getEntry(aOffset, entry);

		BTreeNode node = getNode(entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(ArrayMapEntry aEntry)
	{
		ArrayMapKey key = aEntry.getKey();

		BTreeNode childNode = mChildNodes.get(key);

		if (childNode == null)
		{
			BlockPointer bp = aEntry.getBlockPointer();

			childNode = bp.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(mTree, mLevel - 1, new ArrayMap(mTree.readBlock(bp))) : new BTreeLeafNode(mTree, new ArrayMap(mTree.readBlock(bp)));
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
	protected int size()
	{
		return mChildNodes.size();
	}


	@Override
	protected void scan(ScanResult aScanResult)
	{
		int fillRatio = mChildNodes.getUsedSpace() * 100 / mTree.getNodeSize();
		aScanResult.log.append("{" + (mBlockPointer == null ? "" : mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");

		boolean first = true;
		aScanResult.log.append("'");
		for (ArrayMapEntry entry : mChildNodes)
		{
			if (!first)
			{
				aScanResult.log.append(":");
			}
			first = false;
			String s = stringifyKey(entry.getKey());
			aScanResult.log.append(s.isEmpty() ? "*" : s);
		}
		aScanResult.log.append("'");

		if (mHighlight)
		{
			aScanResult.log.append("#a00#a00#fff");
		}
		else if (size() == 1)
		{
			aScanResult.log.append("#000#ff0#000");
		}
		else if (fillRatio > 100)
		{
			aScanResult.log.append(mModified ? "#a00#a00#fff" : "#666#666#fff");
		}
		else
		{
			aScanResult.log.append(mModified ? "#f00#f00#fff" : "#888#fff#000");
		}

		first = true;
		aScanResult.log.append("[");

		for (int i = 0, sz = size(); i < sz; i++)
		{
			if (!first)
			{
				aScanResult.log.append(",");
			}
			first = false;

			BTreeNode child = getNode(i);

			child.scan(aScanResult);
		}

		aScanResult.log.append("]");
	}
}
