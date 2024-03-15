package org.terifan.raccoon.btree;


class BTreeFlusher
{


//	put()
//	{
//		if (mLevel == 1 ? ((BTreeLeafNode)nearestNode).mMap.getCapacity() > mTree.getLeafSize() || ((BTreeLeafNode)nearestNode).mMap.getFreeSpace() < aEntry.getMarshalledLength() : ((BTreeInteriorNode)nearestNode).getUsedSpace() > mTree.getNodeSize())
//		{
//			ArrayMapKey leftKey = nearestEntry.getKey();
//
//			remove(leftKey);
//
//			SplitResult split = nearestNode.split();
//
//			ArrayMapKey rightKey = split.getRightKey();
//
//			put(leftKey, split.getLeftNode());
//			put(rightKey, split.getRightNode());
//
//			insertEntry(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
//			insertEntry(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
//
//			nearestEntry = new ArrayMapEntry(aKey);
//			loadNearestEntry(nearestEntry);
//			nearestNode = getNode(nearestEntry);
//
//			assert size() >= 2;
//		}
//	}
//
//
//	remove()
//	{
//		BTreeNode curntChld = getNode(offset);
//		BTreeNode leftChild = offset == 0 ? null : getNode(offset - 1);
//		BTreeNode rghtChild = offset + 1 == size() ? null : getNode(offset + 1);
//
//		assert ((curntChld == null || curntChld instanceof BTreeInteriorNode) && (leftChild == null || leftChild instanceof BTreeInteriorNode) && (rghtChild == null || rghtChild instanceof BTreeInteriorNode)) || ((curntChld == null || curntChld instanceof BTreeLeafNode) && (leftChild == null || leftChild instanceof BTreeLeafNode) && (rghtChild == null || rghtChild instanceof BTreeLeafNode));
//
//		int keyLimit = mLevel == 1 ? 0 : 1;
//		int sizeLimit = mLevel == 1 ? mTree.getLeafSize() : mTree.getNodeSize();
//
//		if (leftChild != null && (curntChld.size() + leftChild.size()) < sizeLimit || rghtChild != null && (curntChld.size() + rghtChild.size()) < sizeLimit)
//		{
//			offset = mergeNodes(offset, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
//		}
//
//		BTreeNode.RemoveResult result = curntChld.remove(aKey, aOldEntry);
//
//		if (curntChld.mLevel == 0 && ((BTreeLeafNode)curntChld).mMap.size() == 0)
//		{
//			if (offset == 0)
//			{
//				remove(ArrayMapKey.EMPTY);
//
//				ArrayMapEntry firstEntry = mArrayMap.getFirst();
//
//				BTreeNode firstNode = getNode(firstEntry);
//
//				remove(firstEntry.getKey());
//
//				firstEntry.setKey(ArrayMapKey.EMPTY);
//
//				insertEntry(firstEntry, firstNode);
//			}
//			else
//			{
//				ArrayMapEntry targetEntry = getEntry(offset, new ArrayMapEntry());
//
//				remove(targetEntry.getKey());
//			}
//
//			mTree.freeBlock(curntChld.mBlockPointer);
//		}
//	}
//
//
//	putRoot()
//	{
//		if (mRoot instanceof BTreeLeafNode v)
//		{
//			if (v.mMap.getCapacity() > mLeafSize || v.mMap.getFreeSpace() < aEntry.getMarshalledLength())
//			{
//				_upgrade();
//			}
//		}
//		else if (mRoot instanceof BTreeInteriorNode v)
//		{
//			if (v.getUsedSpace() > mNodeSize)
//			{
//				_grow();
//			}
//		}
//
//	}
//
//
//	removeRoot()
//	{
//		if (result == BTreeNode.RemoveResult.REMOVED)
//		{
//			if (mRoot.mLevel > 1 && mRoot.size() == 1)
//			{
//				_shrink();
//			}
//			if (mRoot.mLevel == 1 && mRoot.size() == 1)
//			{
//				_downgrade();
//			}
//		}
//	}
//
//
//	@Override
//	BTreeNode.SplitResult split()
//	{
//		mTree.freeBlock(mBlockPointer);
//
//		ArrayMap[] maps = mMap.split(mTree.getLeafSize());
//
//		BTreeLeafNode left = new BTreeLeafNode(mTree, mParent, maps[0]);
//		BTreeLeafNode rigt = new BTreeLeafNode(mTree, mParent, maps[1]);
//		left.mModified = true;
//		rigt.mModified = true;
//
//		return new BTreeNode.SplitResult(left, rigt, left.mMap.getFirst().getKey(), rigt.mMap.getFirst().getKey());
//	}
//
//
//	/**
//	 * Merge entries in all child nodes into a single LeafNode which is returned.
//	 */
//	BTreeLeafNode downgrade()
//	{
//		assert mLevel == 1;
//
//		BTreeLeafNode newLeaf = getNode(0);
//		newLeaf.mModified = true;
//
//		for (int i = 1; i < size(); i++)
//		{
//			BTreeLeafNode node = getNode(i);
//
//			node.mMap.forEach(e -> newLeaf.mMap.insert(e));
//
//			mTree.freeBlock(node.mBlockPointer);
//		}
//
//		mTree.freeBlock(mBlockPointer);
//
//		return newLeaf;
//	}
//
//
//	private int mergeNodes(int aOffset, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
//	{
//		int newOffset = aOffset;
//
//		boolean a = aLeftChild != null;
//		boolean b = aRghtChild != null;
//
//		if (aCurntChld instanceof BTreeInteriorNode)
//		{
//			if (a)
//			{
//				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aLeftChild).getUsedSpace() <= aSizeLimit;
//			}
//			if (b)
//			{
//				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aRghtChild).getUsedSpace() <= aSizeLimit;
//			}
//
//			if (a && b)
//			{
//				if (((BTreeInteriorNode)aLeftChild).getFreeSpace() < ((BTreeInteriorNode)aRghtChild).getFreeSpace())
//				{
//					a = false;
//				}
//				else
//				{
//					b = false;
//				}
//			}
//		}
//		else
//		{
//			if (a)
//			{
//				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aLeftChild).mMap.getUsedSpace() <= aSizeLimit;
//			}
//			if (b)
//			{
//				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aRghtChild).mMap.getUsedSpace() <= aSizeLimit;
//			}
//
//			if (a && b)
//			{
//				if (((BTreeLeafNode)aLeftChild).mMap.getFreeSpace() < ((BTreeLeafNode)aRghtChild).mMap.getFreeSpace())
//				{
//					a = false;
//				}
//				else
//				{
//					b = false;
//				}
//			}
//		}
//
//		if (mLevel == 1)
//		{
//			if (a)
//			{
//				mergeLeafs(newOffset - 1, (BTreeLeafNode)aLeftChild, (BTreeLeafNode)aCurntChld);
//				newOffset--;
//			}
//			else if (b)
//			{
//				mergeLeafs(newOffset + 1, (BTreeLeafNode)aRghtChild, (BTreeLeafNode)aCurntChld);
//			}
//			if (newOffset <= 0)
//			{
//				clearFirstKey();
//			}
//		}
//		else
//		{
//			if (a)
//			{
//				mergeIndices(newOffset - 1, (BTreeInteriorNode)aLeftChild, (BTreeInteriorNode)aCurntChld);
//				newOffset--;
//			}
//			else if (b)
//			{
//				mergeIndices(newOffset + 1, (BTreeInteriorNode)aRghtChild, (BTreeInteriorNode)aCurntChld);
//			}
//		}
//
//		// update lowest key after a merge
//		if (newOffset > 0)
//		{
//			ArrayMapEntry nearestEntry = getEntry(newOffset, new ArrayMapEntry());
//			ArrayMapKey nearestKey = nearestEntry.getKey();
//
//			ArrayMapKey firstKey = findLowestLeafKey(aCurntChld);
//
//			if (!firstKey.equals(nearestEntry.getKey()))
//			{
//				nearestEntry.setKey(firstKey);
//
//				mArrayMap.remove(newOffset, null);
//				insertEntry(nearestEntry);
//
//				BTreeNode childNode = removeXX(nearestKey);
//				if (childNode != null)
//				{
//					put(firstKey, childNode);
//				}
//			}
//
//			assert size() >= 2;
//		}
//
//		return newOffset;
//	}
//
//
//	@Override
//	SplitResult split()
//	{
//		mTree.freeBlock(mBlockPointer);
//
//		ArrayMap[] maps = split(mTree.getNodeSize());
//
//		BTreeInteriorNode left = new BTreeInteriorNode(mTree, mParent, mLevel, maps[0]);
//		BTreeInteriorNode right = new BTreeInteriorNode(mTree, mParent, mLevel, maps[1]);
//		left.mModified = true;
//		right.mModified = true;
//
//		ArrayMapKey midKey = right.getKey(0);
//
//		for (Map.Entry<ArrayMapKey, BTreeNode> childEntry : entrySet())
//		{
//			BTreeNode childNode = childEntry.getValue();
//			ArrayMapKey key = childEntry.getKey();
//			if (key.compareTo(midKey) < 0)
//			{
//				childNode.mParent = left;
//				left.put(key, childNode);
//			}
//			else
//			{
//				childNode.mParent = right;
//				right.put(key, childNode);
//			}
//		}
//
//		ArrayMapEntry firstRight = right.mArrayMap.getFirst();
//
//		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
//		ArrayMapKey keyRight = firstRight.getKey();
//
//		BTreeNode firstChild = right.getNode(firstRight);
//
//		right.remove(keyRight);
//
//		firstRight.setKey(keyLeft);
//
//		right.insertEntry(firstRight);
//		right.put(keyLeft, firstChild);
//
//		assert left.size() >= 2;
//		assert right.size() >= 2;
//
//		return new SplitResult(left, right, keyLeft, keyRight);
//	}
//
//
//	BTreeNode grow()
//	{
//		SplitResult split = split();
//
//		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mParent, mLevel + 1, new ArrayMap(mTree.getNodeSize()));
//		interior.mModified = true;
//		interior.insertEntry(new ArrayMapEntry(split.getLeftKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
//		interior.insertEntry(new ArrayMapEntry(split.getRightKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
//		split.getLeftNode().mParent = interior;
//		split.getRightNode().mParent = interior;
//		interior.put(split.getLeftKey(), split.getLeftNode());
//		interior.put(split.getRightKey(), split.getRightNode());
//
//		clear();
//
//		return interior;
//	}
//
//
//	/**
//	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
//	 */
//	BTreeInteriorNode shrink()
//	{
//		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mParent, mLevel - 1, new ArrayMap(mTree.getNodeSize()));
//		interior.mModified = true;
//
//		for (int i = 0; i < size(); i++)
//		{
//			BTreeInteriorNode node = getNode(i);
//
//			boolean first = i > 0;
//			for (ArrayMapEntry entry : node)
//			{
//				ArrayMapEntry newEntry;
//				if (first)
//				{
//					newEntry = new ArrayMapEntry(getKey(i), entry);
//					first = false;
//				}
//				else
//				{
//					newEntry = entry;
//				}
//
//				BTreeNode childNode = node.get(entry.getKey());
//				childNode.mParent = interior;
//				interior.insertEntry(newEntry, childNode);
//			}
//
//			mTree.freeBlock(node.mBlockPointer);
//		}
//
//		mTree.freeBlock(mBlockPointer);
//
//		return interior;
//	}
//
//
//	private void mergeIndices(int aFromIndex, BTreeInteriorNode aFrom, BTreeInteriorNode aTo)
//	{
//		ArrayMapEntry temp = new ArrayMapEntry();
//
//		fixFirstKey(aFrom);
//		fixFirstKey(aTo);
//
//		for (int i = 0, sz = aFrom.size(); i < sz; i++)
//		{
//			aFrom.getEntry(i, temp);
//
//			BTreeNode node = aFrom.getNode(temp);
//			node.mParent = aTo;
//
//			aTo.insertEntry(temp);
//			aTo.put(temp.getKey(), node);
//		}
//
//		aFrom.clearEntries();
//		aFrom.clear();
//		mTree.freeBlock(aFrom.mBlockPointer);
//
//		aTo.clearFirstKey();
//
//		remove(aFromIndex);
//
//		if (aFromIndex == 0)
//		{
//			clearFirstKey();
//		}
//
//		aTo.mModified = true;
//	}
//
//
//	private void mergeLeafs(int aIndex, BTreeLeafNode aFrom, BTreeLeafNode aTo)
//	{
//		ArrayMapEntry temp = new ArrayMapEntry();
//
//		for (int i = 0, sz = aFrom.size(); i < sz; i++)
//		{
//			aFrom.mMap.get(i, temp);
//			aTo.mMap.insert(temp);
//		}
//
//		aFrom.mMap.clear();
//		mTree.freeBlock(aFrom.mBlockPointer);
//
//		remove(aIndex);
//
//		aTo.mModified = true;
//	}
//
//
//	private void fixFirstKey(BTreeInteriorNode aNode)
//	{
//		ArrayMapEntry firstEntry = aNode.mArrayMap.getFirst();
//
//		assert firstEntry.getKey().get().toString().length() == 0 : firstEntry.getKey();
//
//		BTreeNode firstNode = aNode.getNode(firstEntry);
//
//		firstEntry.setKey(findLowestLeafKey(aNode));
//
//		aNode.remove(ArrayMapKey.EMPTY);
//
//		aNode.insertEntry(firstEntry);
//		aNode.put(firstEntry.getKey(), firstNode);
//	}
//
//
//	private void clearFirstKey(ArrayMap mArrayMap, BTreeNode node)
//	{
//		ArrayMapEntry firstEntry = mArrayMap.getFirst();
//
//		if (firstEntry.getKey().size() > 0)
//		{
//			BTreeNode childNode = removeFirst(node);
//
//			firstEntry.setKey(ArrayMapKey.EMPTY);
//
//			insertEntry(firstEntry);
//
//			if (childNode != null)
//			{
//				put(ArrayMapKey.EMPTY, childNode);
//			}
//		}
//	}
//
//
//	private void insertEntry(ArrayMapEntry aArrayMapEntry, BTreeNode aChildNode)
//	{
//		assert aChildNode.mParent == this;
//		mArrayMap.insert(aArrayMapEntry);
//		mChildren.put(aArrayMapEntry.getKey(), aChildNode);
//	}
//
//
//	private BTreeNode removeFirst()
//	{
//		Map.Entry<ArrayMapKey, BTreeNode> tmp = mChildren.firstEntry();
//		mArrayMap.removeFirst();
//		mChildren.remove(tmp.getKey());
//		return tmp.getValue();
//	}
//
//
//	private ArrayMapEntry getLast()
//	{
//		return mArrayMap.getLast();
//	}
//
//
//	private void clearEntries()
//	{
//		mArrayMap.clear();
//	}


//	private static ArrayMapKey findLowestLeafKey(BTreeNode aNode)
//	{
//		if (aNode instanceof BTreeInteriorNode v)
//		{
//			for (int i = 0; i < v.size(); i++)
//			{
//				ArrayMapKey b = findLowestLeafKey(v.getNode(i));
//				if (b.size() > 0)
//				{
//					return b;
//				}
//			}
//			throw new IllegalStateException();
//		}
//
//		BTreeLeafNode leaf = (BTreeLeafNode)aNode;
//
//		if (leaf.mMap.isEmpty())
//		{
//			return ArrayMapKey.EMPTY;
//		}
//
//		return leaf.mMap.getKey(0);
//	}
//
//
//	static class SplitResult
//	{
//		private final BTreeNode mLeftNode;
//		private final BTreeNode mRightNode;
//		private final ArrayMapKey mLeftKey;
//		private final ArrayMapKey mRightKey;
//
//
//		SplitResult(BTreeNode aLeftNode, BTreeNode aRightNode, ArrayMapKey aLeftKey, ArrayMapKey aRightKey)
//		{
//			mLeftNode = aLeftNode;
//			mRightNode = aRightNode;
//			mLeftKey = aLeftKey;
//			mRightKey = aRightKey;
//		}
//
//
//		public BTreeNode getLeftNode()
//		{
//			return mLeftNode;
//		}
//
//
//		public ArrayMapKey getLeftKey()
//		{
//			return mLeftKey;
//		}
//
//
//		public BTreeNode getRightNode()
//		{
//			return mRightNode;
//		}
//
//
//		public ArrayMapKey getRightKey()
//		{
//			return mRightKey;
//		}
//	}
}
