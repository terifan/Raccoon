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

		if (mLevel > 1 && node.mMap.size() < 5)
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


	private void merge(int aIndex, BTreeIndex aFrom, BTreeIndex aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.mMap.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);

			MarshalledKey key = new MarshalledKey(temp.getKey());

			if (key.size() == 0)
			{
				MarshalledKey dstKey = findFirstKey(aFrom);
				temp.setKey(dstKey.marshall());
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
