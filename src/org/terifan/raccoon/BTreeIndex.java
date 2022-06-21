package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex(BTreeTableImplementation aImplementation, BTreeIndex aParent, int aLevel)
	{
		super(aImplementation, aParent);

		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
		setLevel(aLevel);
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
		System.out.println("put");

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

		mMap.remove(nearestEntry, null);

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

		nearestNode.remove(aKey, aOldEntry);

		if (nearestNode.mMap.isEmpty())
		{
			mChildren.remove(nearestKey);
			mMap.remove(nearestEntry, null);

			if (!mMap.isEmpty())
			{
				ArrayMapEntry first = mMap.getFirst();

				if (first.getKey().length > 0)
				{
					MarshalledKey keyA = MarshalledKey.unmarshall(new byte[0]);
					MarshalledKey keyB = MarshalledKey.unmarshall(first.getKey());

					BTreeNode firstChild = mChildren.get(keyB);

					mChildren.remove(keyB);
					mMap.remove(first, null);

					first.setKey(keyA.marshall());

					if (firstChild != null)
					{
						mChildren.put(keyA, firstChild);
					}
					mMap.put(first, null);
				}
			}
		}

		if (getLevel() > 1 && nearestNode.mMap.size() == 1)
		{
			merge(nearestNode, nearestEntry);
		}

		return false;
	}


	@Override
	Object[] split()
	{
		System.out.println("split index");

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
		b.mMap.remove(first, null);

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
		System.out.println("grow");

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
		b.mMap.remove(first, null);

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
		System.out.println("shrink");

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


	/**
	 * Merge a child node with a neighbouring node
	 */
	void merge(BTreeNode nearestNode, ArrayMapEntry nearestEntry)
	{
		System.out.println("merge");

		if (getLevel() > 1 && nearestNode.mMap.size() == 1)
		{
			int i = indexOf(nearestNode);

			if (i == 0)
			{
				ArrayMapEntry prevEntry = nearestNode.mMap.get(0, new ArrayMapEntry());
				MarshalledKey prevKey = MarshalledKey.unmarshall(nearestEntry.getKey());
				BTreeNode prevChild = ((BTreeIndex)nearestNode).getNode(0);

				BTreeIndex nextNode = getNode(1);

				ArrayMapEntry firstEntry = nextNode.mMap.get(0, new ArrayMapEntry());
				BTreeNode firstNode = nextNode.getNode(0);

				MarshalledKey firstKey = MarshalledKey.unmarshall(mMap.get(1, new ArrayMapEntry()).getKey());
				firstEntry.setKey(firstKey.marshall());

				mChildren.remove(prevKey);
				mMap.remove(nearestEntry, null);

				nextNode.mMap.insert(prevEntry, null);
				nextNode.mChildren.put(prevKey, prevChild);

				nextNode.mMap.insert(firstEntry, null);
				nextNode.mChildren.put(firstKey, firstNode);
			}
			else
			{
				BTreeIndex prevNode = getNode(i - 1);
				ArrayMapEntry prevEntry = prevNode.mMap.get(0, new ArrayMapEntry());

				BTreeNode child = ((BTreeIndex)nearestNode).getNode(0);
				MarshalledKey key = MarshalledKey.unmarshall(nearestEntry.getKey());

				prevEntry.setKey(key.marshall());

				mChildren.remove(key);
				mMap.remove(nearestEntry, null);

				prevNode.mMap.insert(prevEntry, null);
				prevNode.mChildren.put(key, child);
			}
		}
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeaf downgrade()
	{
		assert getLevel() == 1;

		System.out.println("downgrade");

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

		BTreeNode node = mChildren.get(new MarshalledKey(entry.getKey()));

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));
			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this, getLevel()) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));
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
		return "BTreeIndex{" + "mChildren=" + mChildren + ", mMap=" + mMap + '}';
	}
}
