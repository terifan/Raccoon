package org.terifan.raccoon;

import java.util.Map.Entry;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.util.ByteArrayBuffer;
import org.terifan.raccoon.blockdevice.util.Console;
import org.terifan.raccoon.util.Result;


public class BTreeIndex extends BTreeNode
{
	NodeBuffer mChildNodes;


	BTreeIndex(int aLevel)
	{
		super(aLevel);

		mChildNodes = new NodeBuffer();
	}


	@Override
	boolean get(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);

		mMap.loadNearestIndexEntry(entry);

		BTreeNode node = getNode(aImplementation, entry);

		return node.get(aImplementation, aKey, aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		mMap.loadNearestIndexEntry(nearestEntry);
		BTreeNode nearestNode = getNode(aImplementation, nearestEntry);

		if (mLevel == 1 ? nearestNode.mMap.getCapacity() > aImplementation.getConfiguration().getInt("leafBlockSize") || nearestNode.mMap.getFreeSpace() < aEntry.getMarshalledLength() : nearestNode.mMap.getUsedSpace() > aImplementation.getConfiguration().getInt("intBlockSize"))
		{
			ArrayMapKey leftKey = nearestEntry.getKey();

			mChildNodes.remove(leftKey);
			mMap.remove(leftKey, null);

			SplitResult split = nearestNode.split(aImplementation);

			ArrayMapKey rightKey = split.rightKey();

			mChildNodes.put(leftKey, split.left());
			mChildNodes.put(rightKey, split.right());

			mMap.insert(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			mMap.insert(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

			nearestEntry = new ArrayMapEntry(aKey);
			mMap.loadNearestIndexEntry(nearestEntry);
			nearestNode = getNode(aImplementation, nearestEntry);
		}

		return nearestNode.put(aImplementation, aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey);

		BTreeNode curntChld = getNode(aImplementation, index);
		BTreeNode leftChild = index == 0 ? null : getNode(aImplementation, index - 1);
		BTreeNode rghtChild = index + 1 == mMap.size() ? null : getNode(aImplementation, index + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? aImplementation.getConfiguration().getInt("leafBlockSize") : aImplementation.getConfiguration().getInt("intBlockSize");

		if (leftChild != null && (curntChld.mMap.size() + leftChild.mMap.size()) < sizeLimit || rghtChild != null && (curntChld.mMap.size() + rghtChild.mMap.size()) < sizeLimit)
		{
			index = mergeNodes(aImplementation, index, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aImplementation, aKey, aOldEntry);

		if (curntChld.mLevel == 0 && curntChld.mMap.size() == 0)
		{
			if (index == 0)
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
				ArrayMapEntry targetEntry = mMap.get(index, new ArrayMapEntry());

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
	void visit(BTree aImplementation, BTreeVisitor aVisitor)
	{
		aVisitor.anyNode(aImplementation, this);
		aVisitor.beforeIndex(aImplementation, this);

		for (int i = 0; i < mMap.size(); i++)
		{
			getNode(aImplementation, i).visit(aImplementation, aVisitor);
		}

		aVisitor.afterIndex(aImplementation, this);
	}


	private int mergeNodes(BTree aImplementation, int aIndex, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
		int newIndex = aIndex;

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
				mergeLeafs(aImplementation, newIndex - 1, (BTreeLeaf)aLeftChild, (BTreeLeaf)aCurntChld);
				newIndex--;
			}
			else if (b)
			{
				mergeLeafs(aImplementation, newIndex + 1, (BTreeLeaf)aRghtChild, (BTreeLeaf)aCurntChld);
			}
			if (newIndex <= 0)
			{
				clearFirstKey();
			}
		}
		else
		{
			if (a)
			{
				mergeIndices(aImplementation, newIndex - 1, (BTreeIndex)aLeftChild, (BTreeIndex)aCurntChld);
				newIndex--;
			}
			else if (b)
			{
				mergeIndices(aImplementation, newIndex + 1, (BTreeIndex)aRghtChild, (BTreeIndex)aCurntChld);
			}
		}

		// update lowest key after a merge
		if (newIndex > 0)
		{
			ArrayMapEntry nearestEntry = mMap.get(newIndex, new ArrayMapEntry());
			ArrayMapKey nearestKey = nearestEntry.getKey();

			ArrayMapKey firstKey = findLowestLeafKey(aImplementation, aCurntChld);

			if (!firstKey.equals(nearestEntry.getKey()))
			{
				nearestEntry.setKey(firstKey);

				mMap.remove(newIndex, null);
				mMap.insert(nearestEntry);

				BTreeNode childNode = mChildNodes.remove(nearestKey);
				if (childNode != null)
				{
					mChildNodes.put(firstKey, childNode);
				}
			}
		}

		return newIndex;
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("intBlockSize"));

		BTreeIndex left = new BTreeIndex(mLevel);
		left.mMap = maps[0];
		left.mModified = true;

		BTreeIndex right = new BTreeIndex(mLevel);
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

		BTreeIndex index = new BTreeIndex(mLevel + 1);
		index.mModified = true;
		index.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("intBlockSize"));
		index.mMap.insert(new ArrayMapEntry(split.leftKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		index.mMap.insert(new ArrayMapEntry(split.rightKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		index.mChildNodes.put(split.leftKey(), split.left());
		index.mChildNodes.put(split.rightKey(), split.right());

		mChildNodes.clear();

		return index;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeIndex shrink(BTree aImplementation)
	{
		BTreeIndex index = new BTreeIndex(mLevel - 1);
		index.mModified = true;
		index.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("intBlockSize"));

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeIndex node = getNode(aImplementation, i);

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

				index.mMap.insert(newEntry);

				BTreeNode childNode = node.mChildNodes.remove(entry.getKey());
				if (childNode != null)
				{
					index.mChildNodes.put(newEntry.getKey(), childNode);
				}
			}

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void mergeIndices(BTree aImplementation, int aFromIndex, BTreeIndex aFrom, BTreeIndex aTo)
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


	private void mergeLeafs(BTree aImplementation, int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp);
		}

		aFrom.mMap.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aIndex, temp);
		mMap.remove(aIndex, null);

		mChildNodes.remove(temp.getKey());

		aTo.mModified = true;
	}


	private void fixFirstKey(BTree aImplementation, BTreeIndex aNode)
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
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex node = (BTreeIndex)aNode;
			for (int i = 0; i < node.mMap.size() ; i++)
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
		if (aNode.mMap.isEmpty()) return ArrayMapKey.EMPTY;

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade(BTree aImplementation)
	{
		assert mLevel == 1;

		BTreeLeaf newLeaf = getNode(aImplementation, 0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeaf node = getNode(aImplementation, i);

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
			assert RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			assert RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(mMap.array(), mLevel, BlockType.TREE_INDEX);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		for (BTreeNode node : mChildNodes.values())
		{
			node.postCommit();
		}

		mChildNodes.clear();

		mModified = false;
	}


	int size()
	{
		return mMap.size();
	}


	<T extends BTreeNode> T getNode(BTree aImplementation, int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		BTreeNode node = getNode(aImplementation, entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(BTree aImplementation, ArrayMapEntry aEntry)
	{
		ArrayMapKey key = aEntry.getKey();

		BTreeNode childNode = mChildNodes.get(key);

		if (childNode == null)
		{
			BlockPointer bp = new BlockPointer().putAll(aEntry.getValue());

			childNode = bp.getBlockType() == BlockType.TREE_INDEX ? new BTreeIndex(mLevel - 1) : new BTreeLeaf();
			childNode.mBlockPointer = bp;
			childNode.mMap = new ArrayMap(aImplementation.readBlock(bp));

			mChildNodes.put(key, childNode);

			assert RuntimeDiagnostics.collectStatistics(bp.getBlockType() == BlockType.TREE_INDEX ? Operation.READ_NODE : Operation.READ_LEAF, 1);
		}

		return childNode;
	}


	@Override
	public String toString()
	{
		String s = Console.format("BTreeIndex{mLevel=%d, mMap=" + mMap + ", mBuffer={", mLevel);
		for (ArrayMapKey t : mChildNodes.keySet())
		{
			s += Console.format("\"%s\",", t);
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
