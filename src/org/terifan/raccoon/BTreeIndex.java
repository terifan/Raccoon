package org.terifan.raccoon;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.ArrayMap.InsertResult;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import static org.terifan.raccoon.BTreeTableImplementation.INDEX_SIZE;


public class BTreeIndex extends BTreeNode
{
	private TreeMap<MarshalledKey, BTreeNode> mBuffer;


	BTreeIndex(int aLevel, long aNodeId)
	{
		super(aLevel, aNodeId);

		mBuffer = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(BTreeTableImplementation mImplementation, MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.array());

		mMap.loadNearestIndexEntry(entry);

		BTreeNode node = getNode(mImplementation, entry);

		return node.get(mImplementation, aKey, aEntry);
	}


	@Override
	InsertResult put(BTreeTableImplementation mImplementation, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.array());
		mMap.loadNearestIndexEntry(nearestEntry);

		MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

		BTreeNode node = getNode(mImplementation, nearestEntry);

		if (node.put(mImplementation, aKey, aEntry, aResult) == InsertResult.PUT)
		{
			return InsertResult.PUT;
		}

		assert Arrays.equals(nearestKey.array(), nearestEntry.getKey());

		mMap.remove(nearestEntry.getKey(), null);

		SplitResult split = node.split(mImplementation);

		MarshalledKey rightKey = split.rightKey();

		putBuffer(nearestKey, split.left());
		putBuffer(rightKey, split.right());

		boolean overflow = false;
		overflow |= mMap.insert(new ArrayMapEntry(nearestKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;
		overflow |= mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null) == InsertResult.RESIZED;

		return overflow ? InsertResult.RESIZED : InsertResult.PUT;
	}


	@Override
	RemoveResult remove(BTreeTableImplementation mImplementation, MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey.array());

		BTreeNode curntChld = getNode(mImplementation, index);

		RemoveResult result = curntChld.remove(mImplementation, aKey, aOldEntry);

		if (result == RemoveResult.NONE)
		{
			return result;
		}

		ArrayMapEntry oldEntry = new ArrayMapEntry();
		mMap.get(index, oldEntry);

		MarshalledKey oldKey = new MarshalledKey(oldEntry.getKey());

		if (curntChld.mMap.size() == 0)
		{
			mMap.remove(index, null);
			mBuffer.remove(oldKey);

			if (index == 0)
			{
				clearFirstKey(this);
			}

			return RemoveResult.OK;
		}

		assert assertValidCache() == null : assertValidCache();

		BTreeNode leftChild = index == 0 ? null : getNode(mImplementation, index - 1);
		BTreeNode rghtChild = index == mMap.size() - 1 ? null : getNode(mImplementation, index + 1);

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? BTreeTableImplementation.LEAF_SIZE : BTreeTableImplementation.INDEX_SIZE;

		boolean a = leftChild != null;
		if (a)
		{
			a &= leftChild.mMap.size() <= keyLimit || curntChld.mMap.size() <= keyLimit || curntChld.mMap.getUsedSpace() + leftChild.mMap.getUsedSpace() <= sizeLimit;
		}

		boolean b = rghtChild != null;
		if (b)
		{
			b &= rghtChild.mMap.size() <= keyLimit || curntChld.mMap.size() <= keyLimit || curntChld.mMap.getUsedSpace() + rghtChild.mMap.getUsedSpace() <= sizeLimit;
		}

		if (a && b)
		{
			if (leftChild.mMap.getFreeSpace() < rghtChild.mMap.getFreeSpace())
			{
				a = false;
			}
			else
			{
				b = false;
			}
		}

		int z = 0;
		if (mLevel == 1)
		{
			if (a)
			{
				z=1;
				mergeLeafs(mImplementation, index - 1, (BTreeLeaf)leftChild, (BTreeLeaf)curntChld);
				index--;
			}
			else if (b)
			{
				z=2;
				mergeLeafs(mImplementation, index + 1, (BTreeLeaf)rghtChild, (BTreeLeaf)curntChld);
			}

			if (index == 0)
			{
				clearFirstKey(this);
			}
		}
		else
		{
			if (a)
			{
				z=3;
				mergeIndices(mImplementation, index - 1, (BTreeIndex)leftChild, index, (BTreeIndex)curntChld);
				index--;
			}
			else if (b)
			{
				z=4;
				mergeIndices(mImplementation, index + 1, (BTreeIndex)rghtChild, index, (BTreeIndex)curntChld);
			}
		}

		if (index > 0)
		{
			byte[] firstKeyBytes = findFirstKey(mImplementation, curntChld);

			MarshalledKey firstKey = new MarshalledKey(firstKeyBytes);

			ArrayMapEntry firstEntry = new ArrayMapEntry(firstKeyBytes);
			get(mImplementation, firstKey, firstEntry);

			oldEntry.setKey(firstEntry.getKey());

			mMap.remove(index, null);
			BTreeNode oldNode = mBuffer.remove(oldKey);

			mMap.insert(oldEntry, null);
			putBuffer(firstKey, oldNode);
		}

		if (mLevel > 1 && curntChld.mMap.getUsedSpace() > sizeLimit)
		{
			MarshalledKey leftKey = new MarshalledKey(findFirstKey(mImplementation, curntChld));

			SplitResult split = curntChld.split(mImplementation);

			MarshalledKey rightKey = split.rightKey();

			mMap.remove(index, null);

			putBuffer(leftKey, split.left());
			putBuffer(rightKey, split.right());

			mMap.insert(new ArrayMapEntry(leftKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			mMap.insert(new ArrayMapEntry(rightKey.array(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			clearFirstKey(this);
		}

		ArrayMapEntry temp = new ArrayMapEntry();
		mMap.get(0, temp);
		if (temp.getKey().length != 0)
		{
			throw new IllegalStateException(mMap.toString());
		}

		return result;
	}


	@Override
	SplitResult split(BTreeTableImplementation mImplementation)
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mLevel, mImplementation.nextNodeIndex());
		BTreeIndex right = new BTreeIndex(mLevel, mImplementation.nextNodeIndex());
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
				left.putBuffer(entry.getKey(), entry.getValue());
			}
			else
			{
				right.putBuffer(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(mImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight, null);
		right.putBuffer(keyLeft, firstChild);

		mBuffer.clear();

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow(BTreeTableImplementation mImplementation)
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(INDEX_SIZE);

		BTreeIndex left = new BTreeIndex(mLevel, mImplementation.nextNodeIndex());
		BTreeIndex right = new BTreeIndex(mLevel, mImplementation.nextNodeIndex());
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
				left.putBuffer(entry.getKey(), entry.getValue());
			}
			else
			{
				right.putBuffer(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = new MarshalledKey(new byte[0]);
		MarshalledKey keyRight = new MarshalledKey(firstRight.getKey());

		BTreeNode firstChild = right.getNode(mImplementation, firstRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mBuffer.remove(keyRight);

		firstRight.setKey(keyLeft.array());

		right.mMap.insert(firstRight, null);
		right.putBuffer(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mLevel + 1, mImplementation.nextNodeIndex());
		index.mMap = new ArrayMap(INDEX_SIZE);
		index.mMap.insert(new ArrayMapEntry(keyLeft.array(), POINTER_PLACEHOLDER, (byte)0x99), null);
		index.mMap.insert(new ArrayMapEntry(keyRight.array(), POINTER_PLACEHOLDER, (byte)0x22), null);
		index.putBuffer(keyLeft, left);
		index.putBuffer(keyRight, right);
		index.mModified = true;

		mBuffer.clear();

		return index;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeIndex shrink(BTreeTableImplementation mImplementation)
	{
		BTreeIndex index = new BTreeIndex(mLevel - 1, mImplementation.nextNodeIndex());
		index.mModified = true;
		index.mMap = new ArrayMap(INDEX_SIZE);

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeIndex node = getNode(mImplementation, i);

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
					index.putBuffer(new MarshalledKey(newEntry.getKey()), child);
				}
			}

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return index;
	}


	private void mergeIndices(BTreeTableImplementation mImplementation, int aFromIndex, BTreeIndex aFrom, int aToIndex, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(mImplementation, aTo);
		fixFirstKey(mImplementation, aFrom);

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			BTreeNode node = aFrom.getNode(mImplementation, temp);

			aTo.mMap.insert(temp, null);
			aTo.putBuffer(new MarshalledKey(temp.getKey()), node);
		}

		aFrom.mMap.clear();
		aFrom.mBuffer.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

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


	private void fixFirstKey(BTreeTableImplementation mImplementation, BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = new ArrayMapEntry();
		aNode.mMap.get(0, firstEntry);

		assert firstEntry.getKey().length == 0;

		BTreeNode firstNode = aNode.getNode(mImplementation, firstEntry);

		firstEntry.setKey(findFirstKey(mImplementation, aNode));

		aNode.mMap.remove(new byte[0], null);
		aNode.mBuffer.remove(new MarshalledKey(new byte[0]));

		aNode.mMap.insert(firstEntry, null);
		aNode.putBuffer(new MarshalledKey(firstEntry.getKey()), firstNode);
	}


	private void clearFirstKey(BTreeIndex aNode)
	{
		ArrayMapEntry firstEntry = aNode.mMap.removeFirst();
		BTreeNode firstNode = aNode.mBuffer.remove(new MarshalledKey(firstEntry.getKey()));

		firstEntry.setKey(new byte[0]);

		aNode.mMap.insert(firstEntry, null);
		aNode.putBuffer(new MarshalledKey(new byte[0]), firstNode);
	}


	private void mergeLeafs(BTreeTableImplementation mImplementation, int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.get(aIndex, temp);
		mMap.remove(aIndex, null);

		mBuffer.remove(new MarshalledKey(temp.getKey()));

		aTo.mModified = true;
	}


	private byte[] findFirstKey(BTreeTableImplementation mImplementation, BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex node)
		{
			return findFirstKey(mImplementation, node.getNode(mImplementation, 0));
		}

		assert aNode.mMap.size() > 0;

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade(BTreeTableImplementation mImplementation)
	{
		assert mLevel == 1;

		BTreeLeaf newLeaf = getNode(mImplementation, 0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeaf node = getNode(mImplementation, i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e, null));

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return newLeaf;
	}


	BTreeNode getBuffer(MarshalledKey aKey)
	{
		return mBuffer.get(aKey);
	}


	void putBuffer(MarshalledKey aKey, BTreeNode aNode)
	{
//		System.out.printf(CCC.CYAN + "Write cache %3d %15s = %s" + CCC.RESET + "%n", mNodeId, "\"" + aKey + "\"", aNode);
		mBuffer.put(aKey, aNode);
	}


	@Override
	boolean commit(BTreeTableImplementation mImplementation, TransactionGroup mTransactionGroup)
	{
		for (Entry<MarshalledKey, BTreeNode> entry : mBuffer.entrySet())
		{
			if (entry.getValue().commit(mImplementation, mTransactionGroup))
			{
				mModified = true;

				mMap.insert(new ArrayMapEntry(entry.getKey().array(), entry.getValue().mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), (byte)0x99), null);
			}

			entry.getValue().mModified = false;
		}

		mBuffer.clear();

//		if (!mModified)
//		{
//			return false;
//		}

		mImplementation.freeBlock(mBlockPointer);

		mBlockPointer = mImplementation.writeBlock(mTransactionGroup, mMap.array(), mLevel, BlockType.INDEX);

		mModified = false;

		return true;
	}


	<T extends BTreeNode> T getNode(BTreeTableImplementation mImplementation, int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		BTreeNode node = getNode(mImplementation, entry);

		return (T)node;
	}


	private BTreeNode getNode(BTreeTableImplementation mImplementation, ArrayMapEntry aEntry)
	{
		MarshalledKey key = new MarshalledKey(aEntry.getKey());

		BTreeNode node = mBuffer.get(key);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mLevel - 1, mImplementation.nextNodeIndex()) : new BTreeLeaf(mImplementation.nextNodeIndex());
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			putBuffer(key, node);
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
