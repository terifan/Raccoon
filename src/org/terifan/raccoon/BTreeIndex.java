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
		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestIndexEntry(nearestEntry);

		MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

		BTreeNode nearestNode = mChildren.get(nearestKey);

		if (nearestNode == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(nearestEntry.getValue()));

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, getLevel() - 1) : new BTreeLeaf(mImplementation, this);
			nearestNode.mBlockPointer = bp;
			nearestNode.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(nearestKey, nearestNode);
		}

		return nearestNode.get(aKey, aEntry);
	}


	@Override
	boolean put(MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestIndexEntry(nearestEntry);

		MarshalledKey nearestKey = new MarshalledKey(nearestEntry.getKey());

		BTreeNode nearestNode = mChildren.get(nearestKey);

		if (nearestNode == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(nearestEntry.getValue()));

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, getLevel() - 1) : new BTreeLeaf(mImplementation, this);
			nearestNode.mBlockPointer = bp;
			nearestNode.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(nearestKey, nearestNode);
		}

		if (!nearestNode.put(aKey, aEntry, aResult))
		{
//			if (new Random().nextBoolean())
//				commit();

			return false;
		}

		mMap.remove(nearestEntry.getKey(), null);

		Object[] split = nearestNode.split();

		MarshalledKey rightKey = (MarshalledKey)split[2];

		mChildren.put(nearestKey, ((BTreeNode)split[0]));
		mChildren.put(rightKey, ((BTreeNode)split[1]));

		boolean overflow = false;
		overflow |= !mMap.insert(new ArrayMapEntry(nearestKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
		overflow |= !mMap.insert(new ArrayMapEntry(rightKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

		return mMap.size() > 3 && overflow;
	}


	@Override
	boolean remove(MarshalledKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		int index = mMap.nearestIndexEntry(new ArrayMapEntry(aKey.marshall()));

		BTreeNode node = getNode(index);

		node.remove(aKey, aOldEntry);

		if (getLevel() == 1 && node.mMap.size() < 5)
		{
			if (index == 0)
			{
				merge(getNode(index + 1), (BTreeLeaf)node);
				mMap.remove(index + 1, null);
			}
			else if (index == mMap.size() - 1)
			{
				merge((BTreeLeaf)node, getNode(index - 1));
				mMap.remove(index, null);
			}
			else
			{
				BTreeLeaf leftChild = getNode(index - 1);
				BTreeLeaf rightChild = getNode(index + 1);

//				if (leftChild.mMap.size() < rightChild.mMap.size())
				if (leftChild.mMap.getFreeSpace() > rightChild.mMap.getFreeSpace())
				{
					merge((BTreeLeaf)node, leftChild);
					mMap.remove(index, null);
				}
				else
				{
					merge(rightChild, (BTreeLeaf)node);
					mMap.remove(index + 1, null);
				}
			}
		}

		if (mLevel > 1 && node.mMap.size() < 5)
		{
			if (index == 0)
			{
				merge(getNode(index + 1), (BTreeIndex)node);
				mMap.remove(index + 1, null);
			}
			else if (index == mMap.size() - 1)
			{
				merge((BTreeIndex)node, getNode(index - 1));
				mMap.remove(index, null);
			}
			else
			{
				BTreeIndex leftChild = getNode(index - 1);
				BTreeIndex rightChild = getNode(index + 1);

//				if (leftChild.mMap.size() < rightChild.mMap.size())
				if (leftChild.mMap.getFreeSpace() > rightChild.mMap.getFreeSpace())
				{
					merge((BTreeIndex)node, leftChild);
					mMap.remove(index, null);
				}
				else
				{
					merge(rightChild, (BTreeIndex)node);
					mMap.remove(index + 1, null);
				}
			}
		}

		return false;
	}


	@Override
	Object[] split()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mIndexSize);

		BTreeIndex a = new BTreeIndex(mImplementation, this, getLevel());
		BTreeIndex b = new BTreeIndex(mImplementation, this, getLevel());
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
		maps[1].get(0, midKeyBytes);
		MarshalledKey midKey = new MarshalledKey(midKeyBytes.getKey());

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				a.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				b.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry first = b.mMap.getFirst();

		MarshalledKey keyA = MarshalledKey.unmarshall(new byte[0]);
		MarshalledKey keyB = MarshalledKey.unmarshall(first.getKey());

		BTreeNode firstChild = b.mChildren.get(keyB);

		b.mChildren.remove(keyB);
		b.mMap.remove(first.getKey(), null);

		first.setKey(keyA.marshall());

		b.mChildren.put(keyA, firstChild);
		b.mMap.put(first, null);

		return new Object[]
		{
			a, b, keyB
		};
	}


	BTreeNode grow()
	{
		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mIndexSize);

		BTreeIndex a = new BTreeIndex(mImplementation, this, getLevel());
		BTreeIndex b = new BTreeIndex(mImplementation, this, getLevel());
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
		b.mMap.get(0, midKeyBytes);

		MarshalledKey midKey = new MarshalledKey(midKeyBytes.getKey());

		for (Entry<MarshalledKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getKey().compareTo(midKey) < 0)
			{
				a.mChildren.put(entry.getKey(), entry.getValue());
			}
			else
			{
				b.mChildren.put(entry.getKey(), entry.getValue());
			}
		}

		ArrayMapEntry first = b.mMap.getFirst();

		MarshalledKey keyA = MarshalledKey.unmarshall(new byte[0]);
		MarshalledKey keyB = MarshalledKey.unmarshall(first.getKey());

		BTreeNode firstChild = b.mChildren.get(keyB);

		b.mChildren.remove(keyB);
		b.mMap.remove(first.getKey(), null);

		first.setKey(keyA.marshall());

		b.mChildren.put(keyA, firstChild);
		b.mMap.put(first, null);

		BTreeIndex newIndex = new BTreeIndex(mImplementation, this, getLevel() + 1);
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);
		newIndex.mModified = true;

		return newIndex;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index nodex which is returned.
	 */
	BTreeIndex shrink()
	{
		BTreeIndex newIndex = new BTreeIndex(mImplementation, mParent, getLevel() - 1);
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
					newEntry = new ArrayMapEntry(mMap.get(i, new ArrayMapEntry()).getKey(), entry.getValue(), entry.getType());
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


	private void merge(BTreeIndex aFrom, BTreeIndex aTo)
	{
		while (!aFrom.mMap.isEmpty())
		{
			ArrayMapEntry entry = new ArrayMapEntry();
			aFrom.mMap.get(0, entry);

			MarshalledKey key = MarshalledKey.unmarshall(entry.getKey());

			MarshalledKey dstKey = findFirstKey(aFrom);
			ArrayMapEntry dstEntry = new ArrayMapEntry(dstKey.marshall(), entry.getValue(), entry.getType());

			aTo.mMap.insert(dstEntry, null);
			aTo.mChildren.put(dstKey, aFrom.mChildren.get(key));

			aFrom.mMap.remove(entry.getKey(), null);
			aFrom.mChildren.remove(key, null);
		}
	}


	private void merge(BTreeLeaf aFrom, BTreeLeaf aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int j = 0; j < aFrom.mMap.size(); j++)
		{
			aFrom.mMap.get(j, temp);

			aTo.mMap.insert(temp, null);
		}

		aFrom.mMap.clear();
	}


	private MarshalledKey findFirstKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeLeaf)
		{
			return new MarshalledKey(((BTreeLeaf)aNode).mMap.get(0, new ArrayMapEntry()).getKey());
		}

		return findFirstKey(((BTreeIndex)aNode).getNode(0));
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade()
	{
		assert getLevel() == 1;

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

		mBlockPointer = mImplementation.writeBlock(mMap.array(), getLevel(), BlockType.INDEX);

		mModified = false;

		return true;
	}


	<T extends BTreeNode> T getNode(int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		MarshalledKey key = new MarshalledKey(entry.getKey());

		BTreeNode node = mChildren.get(key);

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));

			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, getLevel() - 1) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(key, node);
		}

		return (T)node;
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
		return "BTreeIndex{mLevel=" + getLevel() + ", mMap=" + mMap + ", mChildren=" + mChildren.keySet() + '}';
	}
}
