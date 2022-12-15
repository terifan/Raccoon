package org.terifan.raccoon;

import java.util.Arrays;
import java.util.Map.Entry;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.ArrayMap.PutResult;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTree.BLOCKPOINTER_PLACEHOLDER;
import org.terifan.raccoon.util.Console;


public class BTreeIndex extends BTreeNode
{
	NodeBuffer mChildNodes;


	BTreeIndex(int aLevel)
	{
		super(aLevel);

		mChildNodes = new NodeBuffer();
	}


	@Override
	boolean get(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());

		mMap.loadNearestIndexEntry(entry);

		BTreeNode node = getNode(aImplementation, entry);

		return node.get(aImplementation, aKey, aEntry);
	}


	@Override
	PutResult put(BTree aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;
		BTreeNode nearestNode;

		synchronized (this)
		{
			ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.array());
			mMap.loadNearestIndexEntry(nearestEntry);
			nearestNode = getNode(aImplementation, nearestEntry);

			if (mLevel == 1 ? nearestNode.mMap.getCapacity() > aImplementation.getConfiguration().getInt("leafSize") || nearestNode.mMap.getFreeSpace() < aEntry.getMarshalledLength() : nearestNode.mMap.getUsedSpace() > aImplementation.getConfiguration().getInt("indexSize"))
			{
				MarshalledKey leftKey = new MarshalledKey(nearestEntry.getKey());

				mChildNodes.remove(leftKey);
				mMap.remove(leftKey.array(), null);

				SplitResult split = nearestNode.split(aImplementation);

				MarshalledKey rightKey = split.rightKey();

				mChildNodes.put(leftKey, split.left());
				mChildNodes.put(rightKey, split.right());

				mMap.insert(new ArrayMapEntry(leftKey.array(), BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
				mMap.insert(new ArrayMapEntry(rightKey.array(), BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

				nearestEntry = new ArrayMapEntry(aKey.array());
				mMap.loadNearestIndexEntry(nearestEntry);
				nearestNode = getNode(aImplementation, nearestEntry);
			}
		}

		return nearestNode.put(aImplementation, aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(BTree aImplementation, MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey.array());

		BTreeNode curntChld = getNode(aImplementation, index);
		BTreeNode leftChild = index == 0 ? null : getNode(aImplementation, index - 1);
		BTreeNode rghtChild = index + 1 == mMap.size() ? null : getNode(aImplementation, index + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? aImplementation.getConfiguration().getInt("leafSize") : aImplementation.getConfiguration().getInt("indexSize");

		if (leftChild != null && (curntChld.mMap.size() + leftChild.mMap.size()) < sizeLimit || rghtChild != null && (curntChld.mMap.size() + rghtChild.mMap.size()) < sizeLimit)
		{
			index = mergeNodes(aImplementation, index, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aImplementation, aKey, aOldEntry);

		if (curntChld.mLevel == 0 && curntChld.mMap.size() == 0)
		{
			removeEmptyLeafNode(aImplementation, curntChld, index);
		}

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mMap.get(0, new ArrayMapEntry()).getKey().length == 0 : "First key expected to be empty: " + mMap.toString();

		return result;
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
				clearFirstKey(this);
			}
		}
		else
		{
			if (a)
			{
				mergeIndices(aImplementation, newIndex - 1, (BTreeIndex)aLeftChild, newIndex, (BTreeIndex)aCurntChld);
				newIndex--;
			}
			else if (b)
			{
				mergeIndices(aImplementation, newIndex + 1, (BTreeIndex)aRghtChild, newIndex, (BTreeIndex)aCurntChld);
			}
		}

		// update lowest key after a merge
		if (newIndex > 0)
		{
			ArrayMapEntry nearestEntry = mMap.get(newIndex, new ArrayMapEntry());
			MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

			byte[] firstKeyBytes = findLowestLeafKey(aImplementation, aCurntChld);

			if (!Arrays.equals(firstKeyBytes, nearestEntry.getKey()))
			{
				nearestEntry.setKey(firstKeyBytes);

				mMap.remove(newIndex, null);
				mMap.insert(nearestEntry);

				BTreeNode childNode = mChildNodes.remove(nearestKey);
				if (childNode != null)
				{
					mChildNodes.put(new MarshalledKey(firstKeyBytes), childNode);
				}
			}
		}

		return newIndex;
	}


	@Override
	SplitResult split(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("indexSize"));

		BTreeIndex left = new BTreeIndex(mLevel);
		left.mMap = maps[0];
		left.mModified = true;

		BTreeIndex right = new BTreeIndex(mLevel);
		right.mMap = maps[1];
		right.mModified = true;

		byte[] midKeyBytes = maps[1].getKey(0);
		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> childEntry : mChildNodes.entrySet())
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

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mChildNodes.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight);
		right.mChildNodes.put(keyLeft, firstChild);

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow(BTree aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(aImplementation.getConfiguration().getInt("indexSize"));

		BTreeIndex left = new BTreeIndex(mLevel);
		BTreeIndex right = new BTreeIndex(mLevel);
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = right.mMap.getKey(0);

		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> childEntry : mChildNodes.entrySet())
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

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mChildNodes.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight);
		right.mChildNodes.put(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mLevel + 1);
		index.mModified = true;
		index.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("indexSize"));
		index.mMap.insert(new ArrayMapEntry(keyLeft.array(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		index.mMap.insert(new ArrayMapEntry(keyRight.array(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		index.mChildNodes.put(keyLeft, left);
		index.mChildNodes.put(keyRight, right);

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
		index.mMap = new ArrayMap(aImplementation.getConfiguration().getInt("indexSize"));

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeIndex node = getNode(aImplementation, i);

			boolean first = i > 0;
			for (ArrayMapEntry entry : node.mMap)
			{
				ArrayMapEntry newEntry;
				if (first)
				{
					newEntry = new ArrayMapEntry(mMap.getKey(i), entry.getValue(), entry.getType());
					first = false;
				}
				else
				{
					newEntry = entry;
				}

				index.mMap.insert(newEntry);

				BTreeNode childNode = node.mChildNodes.remove(new MarshalledKey(entry.getKey()));
				if (childNode != null)
				{
					index.mChildNodes.put(new MarshalledKey(newEntry.getKey()), childNode);
				}
			}

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void mergeIndices(BTree aImplementation, int aFromIndex, BTreeIndex aFrom, int aToIndex, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(aImplementation, aTo);
		fixFirstKey(aImplementation, aFrom);

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			BTreeNode node = aFrom.getNode(aImplementation, temp);

			aTo.mMap.insert(temp);
			aTo.mChildNodes.put(new MarshalledKey(temp.getKey()), node);
		}

		aFrom.mMap.clear();
		aFrom.mChildNodes.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		clearFirstKey(aTo);

		mMap.get(aFromIndex, temp);
		mMap.remove(aFromIndex, null);
		mChildNodes.remove(new MarshalledKey(temp.getKey()));

		if (aFromIndex == 0)
		{
			clearFirstKey(this);
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

		mChildNodes.remove(new MarshalledKey(temp.getKey()));

		aTo.mModified = true;
	}


	private void removeEmptyLeafNode(BTree aImplementation, BTreeNode aNode, int aIndex)
	{
		if (aIndex == 0)
		{
			mMap.remove(new byte[0], null);
			mChildNodes.remove(new MarshalledKey(new byte[0]));

			ArrayMapEntry firstEntry = mMap.get(0, new ArrayMapEntry());

			BTreeNode firstNode = getNode(aImplementation, firstEntry);

			mMap.remove(firstEntry.getKey(), null);
			mChildNodes.remove(new MarshalledKey(firstEntry.getKey()));

			firstEntry.setKey(new byte[0]);

			mMap.insert(firstEntry);
			mChildNodes.put(new MarshalledKey(firstEntry.getKey()), firstNode);
		}
		else
		{
			ArrayMapEntry targetEntry = mMap.get(aIndex, new ArrayMapEntry());

			mMap.remove(targetEntry.getKey(), null);
			mChildNodes.remove(new MarshalledKey(targetEntry.getKey()));
		}

		aImplementation.freeBlock(aNode.mBlockPointer);
	}


	private void fixFirstKey(BTree aImplementation, BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.get(0, new ArrayMapEntry());

		assert firstEntry.getKey().length == 0;

		BTreeNode firstNode = aNode.getNode(aImplementation, firstEntry);

		firstEntry.setKey(findLowestLeafKey(aImplementation, aNode));

		aNode.mMap.remove(new byte[0], null);
		aNode.mChildNodes.remove(new MarshalledKey(new byte[0]));

		aNode.mMap.insert(firstEntry);
		aNode.mChildNodes.put(new MarshalledKey(firstEntry.getKey()), firstNode);
	}


	private void clearFirstKey(BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.get(0, new ArrayMapEntry());

		if (firstEntry.getKey().length > 0)
		{
			aNode.mMap.removeFirst();

			BTreeNode childNode = aNode.mChildNodes.remove(new MarshalledKey(firstEntry.getKey()));

			firstEntry.setKey(new byte[0]);

			aNode.mMap.insert(firstEntry);

			if (childNode != null)
			{
				aNode.mChildNodes.put(new MarshalledKey(new byte[0]), childNode);
			}
		}
	}


	private byte[] findLowestLeafKey(BTree aImplementation, BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex)
		{
			BTreeIndex node = (BTreeIndex)aNode;
			for (int i = 0; i < mMap.size() ; i++)
			{
				byte[] b = findLowestLeafKey(aImplementation, node.getNode(aImplementation, i));
				if (b.length > 0)
				{
					return b;
				}
			}
			throw new IllegalStateException();
		}

//		assert !aNode.mMap.isEmpty();
		if (aNode.mMap.isEmpty()) return new byte[0];

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

		for (Entry<MarshalledKey, BTreeNode> entry : mChildNodes.entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit(aImplementation))
			{
				mModified = true;

				mMap.insert(new ArrayMapEntry(entry.getKey().array(), node.mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), TYPE_TREENODE));
			}
		}

		if (mModified)
		{
			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(mMap.array(), mLevel, BlockType.TREE_INDEX);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		for (Entry<MarshalledKey, BTreeNode> entry : mChildNodes.entrySet())
		{
			entry.getValue().postCommit();
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


	BTreeNode getNode(BTree aImplementation, ArrayMapEntry aEntry)
	{
		MarshalledKey key = new MarshalledKey(aEntry.getKey());

		BTreeNode childNode = mChildNodes.get(key);

		if (childNode == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			childNode = bp.getBlockType() == BlockType.TREE_INDEX ? new BTreeIndex(mLevel - 1) : new BTreeLeaf();
			childNode.mBlockPointer = bp;
			childNode.mMap = new ArrayMap(aImplementation.readBlock(bp));

			mChildNodes.put(key, childNode);
		}

		return childNode;
	}


	@Override
	public String toString()
	{
		String s = Console.format("BTreeIndex{mLevel=%d, mMap=" + mMap + ", mBuffer={", mLevel);
		for (MarshalledKey t : mChildNodes.keySet())
		{
			s += Console.format("\"%s\",", t);
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	private String assertValidCache()
	{
		for (MarshalledKey key : mChildNodes.keySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(key.array());
			if (!mMap.get(entry))
			{
				return entry.toString();
			}
		}
		return null;
	}
}
