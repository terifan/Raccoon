package org.terifan.raccoon;

import java.util.Arrays;
import java.util.Map.Entry;
import org.terifan.raccoon.ArrayMap.InsertResult;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTreeTableImplementation.INDEX_SIZE;


public class BTreeIndex extends BTreeNode
{
	public NodeBuffer mBuffer;


	BTreeIndex(int aLevel, long aNodeId)
	{
		super(aLevel, aNodeId);

		mBuffer = new NodeBuffer();
	}


	@Override
	boolean get(BTreeTableImplementation aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());

		mMap.loadNearestIndexEntry(entry);

		BTreeNode node = getNode(aImplementation, entry);

		return node.get(aImplementation, aKey, aEntry);
	}


	@Override
	InsertResult put(BTreeTableImplementation aImplementation, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.array());

		mMap.loadNearestIndexEntry(nearestEntry);

		BTreeNode nearestNode = getNode(aImplementation, nearestEntry);

		if (nearestNode.put(aImplementation, aKey, aEntry, aResult) == InsertResult.PUT)
		{
			return InsertResult.PUT;
		}

		MarshalledKey leftKey = new MarshalledKey(nearestEntry.getKey());

		mBuffer.remove(leftKey);
		mMap.remove(leftKey.array(), null);

		SplitResult split = nearestNode.split(aImplementation);

		MarshalledKey rightKey = split.rightKey();

		mBuffer.put(leftKey, split.left());
		mBuffer.put(rightKey, split.right());

		boolean resized = false;
		resized |= mMap.insert(new ArrayMapEntry(leftKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0), null) == InsertResult.RESIZED;
		resized |= mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0), null) == InsertResult.RESIZED;

		return resized ? InsertResult.RESIZED : InsertResult.PUT;
	}


	@Override
	RemoveResult remove(BTreeTableImplementation aImplementation, MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey.array());

		BTreeNode curntChld = getNode(aImplementation, index);

		RemoveResult result = curntChld.remove(aImplementation, aKey, aOldEntry);

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		ArrayMapEntry nearestEntry = mMap.get(index, new ArrayMapEntry());
		MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

		if (curntChld.mMap.size() == 0)
		{
			assert index == 0;

			mMap.remove(index, null);
			mBuffer.remove(nearestKey);

			if (index == 0)
			{
				clearFirstKey(this);
			}

			assert assertValidCache() == null : assertValidCache();

			return RemoveResult.REMOVED;
		}

		BTreeNode leftChild = index == 0 ? null : getNode(aImplementation, index - 1);
		BTreeNode rghtChild = index + 1 == mMap.size() ? null : getNode(aImplementation, index + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? BTreeTableImplementation.LEAF_SIZE : BTreeTableImplementation.INDEX_SIZE;

		index = mergeNodes(aImplementation, index, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);

		if (index > 0)
		{
			byte[] firstKeyBytes = findLowestKeyInBranch(aImplementation, curntChld);

			MarshalledKey firstKey = new MarshalledKey(firstKeyBytes);

			ArrayMapEntry firstEntry = new ArrayMapEntry(firstKeyBytes);
			get(aImplementation, firstKey, firstEntry);

			nearestEntry.setKey(firstEntry.getKey());

			mMap.remove(index, null);
			BTreeNode oldNode = mBuffer.remove(nearestKey);

			mMap.insert(nearestEntry, null);
			mBuffer.put(firstKey, oldNode);
		}

		if (mLevel > 1 && curntChld.mMap.getUsedSpace() > sizeLimit)
		{
			MarshalledKey leftKey = new MarshalledKey(findLowestKeyInBranch(aImplementation, curntChld));

			SplitResult split = curntChld.split(aImplementation);

			MarshalledKey rightKey = split.rightKey();

			mMap.remove(index, null);

			mBuffer.put(leftKey, split.left());
			mBuffer.put(rightKey, split.right());

			mMap.insert(new ArrayMapEntry(leftKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			clearFirstKey(this);
		}

		assert assertValidCache() == null : assertValidCache();
		assert mMap.get(0, new ArrayMapEntry()).getKey().length == 0 : "First key expected to be empty: " + mMap.toString();

		return result;
	}


	private int mergeNodes(BTreeTableImplementation aImplementation, int aIndex, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
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
				mergeLeafs(aImplementation, aIndex - 1, (BTreeLeaf)aLeftChild, (BTreeLeaf)aCurntChld);
				aIndex--;
			}
			else if (b)
			{
				mergeLeafs(aImplementation, aIndex + 1, (BTreeLeaf)aRghtChild, (BTreeLeaf)aCurntChld);
			}
			if (aIndex <= 0)
			{
				clearFirstKey(this);
			}
		}
		else
		{
			if (a)
			{
				mergeIndices(aImplementation, aIndex - 1, (BTreeIndex)aLeftChild, aIndex, (BTreeIndex)aCurntChld);
				aIndex--;
			}
			else if (b)
			{
				mergeIndices(aImplementation, aIndex + 1, (BTreeIndex)aRghtChild, aIndex, (BTreeIndex)aCurntChld);
			}
		}

		return aIndex;
	}


	@Override
	SplitResult split(BTreeTableImplementation aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mLevel, aImplementation.nextNodeIndex());
		BTreeIndex right = new BTreeIndex(mLevel, aImplementation.nextNodeIndex());
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = maps[1].getKey(0);
		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mBuffer.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mBuffer.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight, null);
		right.mBuffer.put(keyLeft, firstChild);

		mBuffer.clear();

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow(BTreeTableImplementation aImplementation)
	{
		aImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mLevel, aImplementation.nextNodeIndex());
		BTreeIndex right = new BTreeIndex(mLevel, aImplementation.nextNodeIndex());
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = right.mMap.getKey(0);

		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mBuffer.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mBuffer.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(aImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight, null);
		right.mBuffer.put(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mLevel + 1, aImplementation.nextNodeIndex());
		index.mMap = new ArrayMap(INDEX_SIZE);
		index.mMap.insert(new ArrayMapEntry(keyLeft.array(), POINTER_PLACEHOLDER, (byte)0x99), null);
		index.mMap.insert(new ArrayMapEntry(keyRight.array(), POINTER_PLACEHOLDER, (byte)0x22), null);
		index.mBuffer.put(keyLeft, left);
		index.mBuffer.put(keyRight, right);
		index.mModified = true;

		mBuffer.clear();

		return index;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeIndex shrink(BTreeTableImplementation aImplementation)
	{
		BTreeIndex index = new BTreeIndex(mLevel - 1, aImplementation.nextNodeIndex());
		index.mModified = true;
		index.mMap = new ArrayMap(INDEX_SIZE);

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

				index.mMap.insert(newEntry, null);

				BTreeNode child = node.mBuffer.remove(new MarshalledKey(entry.getKey()));
				if (child != null)
				{
					index.mBuffer.put(new MarshalledKey(newEntry.getKey()), child);
				}
			}

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void mergeIndices(BTreeTableImplementation aImplementation, int aFromIndex, BTreeIndex aFrom, int aToIndex, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(aImplementation, aTo);
		fixFirstKey(aImplementation, aFrom);

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			BTreeNode node = aFrom.getNode(aImplementation, temp);

			aTo.mMap.insert(temp, null);
			aTo.mBuffer.put(new MarshalledKey(temp.getKey()), node);
		}

		aFrom.mMap.clear();
		aFrom.mBuffer.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		clearFirstKey(aTo);

		mMap.get(aFromIndex, temp);
		mMap.remove(aFromIndex, null);
		mBuffer.remove(new MarshalledKey(temp.getKey()));

		if (aFromIndex == 0)
		{
			clearFirstKey(this);
		}

		aTo.mModified = true;
	}


	private void mergeLeafs(BTreeTableImplementation aImplementation, int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
		aImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aIndex, temp);
		mMap.remove(aIndex, null);

		mBuffer.remove(new MarshalledKey(temp.getKey()));

		aTo.mModified = true;
	}


	private void fixFirstKey(BTreeTableImplementation aImplementation, BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = new ArrayMapEntry();
		aNode.mMap.get(0, firstEntry);

		assert firstEntry.getKey().length == 0;

		BTreeNode firstNode = aNode.getNode(aImplementation, firstEntry);

		firstEntry.setKey(findLowestKeyInBranch(aImplementation, aNode));

		aNode.mMap.remove(new byte[0], null);
		aNode.mBuffer.remove(new MarshalledKey(new byte[0]));

		aNode.mMap.insert(firstEntry, null);
		aNode.mBuffer.put(new MarshalledKey(firstEntry.getKey()), firstNode);
	}


	private void clearFirstKey(BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.removeFirst();

		BTreeNode firstNode = aNode.mBuffer.remove(new MarshalledKey(firstEntry.getKey()));

		firstEntry.setKey(new byte[0]);

		aNode.mMap.insert(firstEntry, null);
		aNode.mBuffer.put(new MarshalledKey(new byte[0]), firstNode);
	}


	private byte[] findLowestKeyInBranch(BTreeTableImplementation aImplementation, BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex node)
		{
			return findLowestKeyInBranch(aImplementation, node.getNode(aImplementation, 0));
		}

		assert aNode.mMap.size() > 0;

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade(BTreeTableImplementation aImplementation)
	{
		assert mLevel == 1;

		BTreeLeaf newLeaf = getNode(aImplementation, 0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeaf node = getNode(aImplementation, i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e, null));

			aImplementation.freeBlock(node.mBlockPointer);
		}

		aImplementation.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit(BTreeTableImplementation aImplementation, TransactionGroup aTransactionGroup)
	{
		assert assertValidCache() == null : assertValidCache();

		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit(aImplementation, aTransactionGroup))
			{
				mModified = true;

				mMap.insert(new ArrayMapEntry(entry.getKey().array(), node.mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), (byte)0), null);
			}
		}

		if (mModified)
		{
			aImplementation.freeBlock(mBlockPointer);

			mBlockPointer = aImplementation.writeBlock(aTransactionGroup, mMap.array(), mLevel, BlockType.INDEX);

			aImplementation.hasCommitted(this);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			entry.getValue().postCommit();
		}

		mBuffer.clear();

		mModified = false;
	}


	<T extends BTreeNode> T getNode(BTreeTableImplementation aImplementation, int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		BTreeNode node = getNode(aImplementation, entry);

		return (T)node;
	}


	private BTreeNode getNode(BTreeTableImplementation aImplementation, ArrayMapEntry aEntry)
	{
		MarshalledKey key = new MarshalledKey(aEntry.getKey());

		BTreeNode node = mBuffer.get(key);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mLevel - 1, aImplementation.nextNodeIndex()) : new BTreeLeaf(aImplementation.nextNodeIndex());
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(aImplementation.readBlock(bp));

			mBuffer.put(key, node);
		}

		return node;
	}


	@Override
	public String toString()
	{
		String s = "BTreeIndex{mLevel=" + mLevel + ", mMap=" + mMap + ", mBuffer={";
		for (MarshalledKey t : mBuffer.keySet())
		{
			s += "\"" + t + "\",";
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	private String assertValidCache()
	{
		for (MarshalledKey key : mBuffer.keySet())
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
