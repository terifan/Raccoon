package org.terifan.raccoon;

import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;
import test.TestTiny;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex(BTreeTableImplementation aImplementation, BTreeIndex aParent, int aLevel)
	{
		super(aImplementation, aParent);

		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
		mLevel = aLevel;
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestIndexEntry(entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		return node.get(aKey, aEntry);
	}


	@Override
	boolean put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry entry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestIndexEntry(entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		if (!node.put(aKey, aEntry, aResult))
		{
			return false;
		}

		mMap.remove(entry.getKey(), null);

		Object[] split = node.split();

		MarshalledKey rightKey = (MarshalledKey)split[2];

		mChildren.put(key, ((BTreeNode)split[0]));
		mChildren.put(rightKey, ((BTreeNode)split[1]));

		boolean overflow = false;
		overflow |= !mMap.insert(new ArrayMapEntry(key.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
		overflow |= !mMap.insert(new ArrayMapEntry(rightKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

		return mMap.size() > 3 && overflow;
	}


	@Override
	boolean remove(MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int index = mMap.nearestIndex(aKey.marshall());

		BTreeNode node = getNode(index);

		node.remove(aKey, aOldEntry);

		if (mLevel == 1 && node.mMap.size() < 2)
		{
			BTreeLeaf leftChild = index == 0 ? null : getNode(index - 1);
			BTreeLeaf rightChild = index == mMap.size() - 1 ? null : getNode(index + 1);

			if (rightChild == null || leftChild != null && leftChild.mMap.getFreeSpace() > rightChild.mMap.getFreeSpace())
			{
				merge(index, (BTreeLeaf)node, leftChild);
			}
			else
			{
				merge(index + 1, rightChild, (BTreeLeaf)node);
			}
		}

		if (mLevel > 1 && node.mMap.size() < 2)
		{
			BTreeIndex leftChild = index == 0 ? null : getNode(index - 1);
			BTreeIndex rightChild = index == mMap.size() - 1 ? null : getNode(index + 1);

			if (rightChild == null || leftChild != null && leftChild.mMap.getFreeSpace() > rightChild.mMap.getFreeSpace())
			{
				merge(index, (BTreeIndex)node, leftChild);
			}
			else
			{
				merge(index + 1, rightChild, (BTreeIndex)node);
			}
		}

		return false;
	}


	@Override
	Object[] split()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mIndexSize);

		BTreeIndex left = new BTreeIndex(mImplementation, this, mLevel);
		BTreeIndex right = new BTreeIndex(mImplementation, this, mLevel);
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = maps[1].getKey(0);
		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry firstRight = right.mMap.getFirst();

		MarshalledKey keyLeft = MarshalledKey.unmarshall(new byte[0]);
		MarshalledKey keyRight = MarshalledKey.unmarshall(firstRight.getKey());

		BTreeNode firstChild = right.mChildren.get(keyRight);

		right.mMap.remove(firstRight.getKey(), null);
		right.mChildren.remove(keyRight);

		firstRight.setKey(keyLeft.marshall());

		right.mMap.put(firstRight, null);
		right.mChildren.put(keyLeft, firstChild);

		return new Object[]
		{
			left, right, keyRight
		};
	}


	BTreeNode grow()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mIndexSize);

		BTreeIndex left = new BTreeIndex(mImplementation, this, mLevel);
		BTreeIndex right = new BTreeIndex(mImplementation, this, mLevel);
		left.mMap = maps[0];
		right.mMap = maps[1];
		left.mModified = true;
		right.mModified = true;

		byte[] midKeyBytes = right.mMap.getKey(0);

		MarshalledKey midKey = new MarshalledKey(midKeyBytes);

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				left.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				right.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry first = right.mMap.getFirst();

		MarshalledKey keyLeft = MarshalledKey.unmarshall(new byte[0]);
		MarshalledKey keyRight = MarshalledKey.unmarshall(first.getKey());

		BTreeNode firstChild = right.mChildren.get(keyRight);

		right.mMap.remove(first.getKey(), null);
		right.mChildren.remove(keyRight);

		first.setKey(keyLeft.marshall());

		right.mMap.put(first, null);
		right.mChildren.put(keyLeft, firstChild);

		BTreeIndex index = new BTreeIndex(mImplementation, this, mLevel + 1);
		index.mMap = new ArrayMap(mIndexSize);
		index.mMap.put(new ArrayMapEntry(keyLeft.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		index.mMap.put(new ArrayMapEntry(keyRight.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		index.mChildren.put(keyLeft, left);
		index.mChildren.put(keyRight, right);
		index.mModified = true;

		return index;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index nodex which is returned.
	 */
	BTreeIndex shrink()
	{
		BTreeIndex newIndex = new BTreeIndex(mImplementation, mParent, mLevel - 1);
		newIndex.mModified = true;
		newIndex.mMap = new ArrayMap(mIndexSize);

		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeIndex node = getNode(i);

			boolean first = true;
			for (ArrayMapEntry entry : node.mMap)
			{
				ArrayMapEntry newEntry;
				if (first && i > 0)
				{
					newEntry = new ArrayMapEntry(mMap.getKey(i), entry.getValue(), entry.getType());
				}
				else
				{
					newEntry = entry;
				}
				newIndex.mMap.insert(newEntry, null);
				BTreeNode child = node.mChildren.get(new MarshalledKey(entry.getKey()));
				if (child != null)
				{
					newIndex.mChildren.put(new MarshalledKey(newEntry.getKey()), child);
				}
				first = false;
			}

			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return newIndex;
	}


	private void merge(int aIndex, BTreeIndex aFrom, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			MarshalledKey key = new MarshalledKey(temp.getKey());

			if (key.size() == 0)
			{
				temp.setKey(findFirstKey(aFrom));
			}

			aTo.mMap.insert(temp, null);
			aTo.mChildren.put(new MarshalledKey(temp.getKey()), aFrom.mChildren.get(key));
		}

		aFrom.mMap.clear();
		aFrom.mChildren.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.remove(aIndex, null);
	}


	private void merge(int aIndex, BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
		mImplementation.freeBlock(aFrom.mBlockPointer);

		mMap.remove(aIndex, null);
	}


	private byte[] findFirstKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeIndex node)
		{
			return findFirstKey(node.getNode(0));
		}

		return aNode.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade()
	{
		assert mLevel == 1;

		BTreeLeaf newLeaf = getNode(0);
		newLeaf.mModified = true;

		for (int i = 1; i < mMap.size(); i++)
		{
			BTreeLeaf node = getNode(i);
			node.mMap.forEach(e -> newLeaf.mMap.insert(e, null));
			mImplementation.freeBlock(node.mBlockPointer);
		}

		mImplementation.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit()
	{
		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getValue().commit())
			{
				mModified = true;

				mMap.put(new ArrayMapEntry(entry.getKey().marshall(), entry.getValue().mBlockPointer.marshal(ByteArrayBuffer.alloc(BlockPointer.SIZE)).array(), (byte)0x99), null);
			}

			entry.getValue().mModified = false;
		}

		mChildren.clear();

//		if (!mModified)
//		{
//			return false;
//		}

		mImplementation.freeBlock(mBlockPointer);

		mBlockPointer = mImplementation.writeBlock(mMap.array(), mLevel, BlockType.INDEX);

		mModified = false;

		return true;
	}


	<T extends BTreeNode> T getNode(int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = getNode(entry, key);

		return (T)node;
	}


	private BTreeNode getNode(ArrayMapEntry aEntry, MarshalledKey aKey)
	{
		BTreeNode node = mChildren.get(aKey);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aEntry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, mLevel - 1) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(aKey, node);
		}

		return node;
	}


	int indexOf(BTreeNode aTreeNode)
	{
		for (int i = 0; i < mMap.size(); i++)
		{
			BTreeNode node = getNode(i);

			if (node == aTreeNode)
			{
				return i;
			}
		}

		return -1;
	}


	@Override
	public String toString()
	{
		return "BTreeIndex{mLevel=" + mLevel + ", mMap=" + mMap + ", mChildren=" + mChildren.keySet() + '}';
	}
}
