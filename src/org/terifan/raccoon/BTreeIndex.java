package org.terifan.raccoon;

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


	BTreeIndex(BTreeTableImplementation aImplementation, BTreeIndex aParent)
	{
		super(aImplementation, aParent);
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
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

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this) : new BTreeLeaf(mImplementation, this);
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

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this) : new BTreeLeaf(mImplementation, this);
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

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this) : new BTreeLeaf(mImplementation, this);
			nearestNode.mBlockPointer = bp;
			nearestNode.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(nearestKey, nearestNode);
		}

		boolean b = nearestNode.remove(aKey, aOldEntry);

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

		if (mMap.size() == 1)
		{
			BTreeIndex prev = null;
			BTreeIndex next = null;
			ArrayMapEntry entry = null;
			for (int i = 0; i < mParent.mMap.size(); i++)
			{
				entry = new ArrayMapEntry();
				mParent.mMap.get(i, entry);
				BTreeNode node = mParent.mChildren.get(new MarshalledKey(entry.getKey()));
				if (node == this)
				{
					if (prev == null)
					{
						ArrayMapEntry tmp = new ArrayMapEntry();
						mParent.mMap.get(i + 1, tmp);
						next = (BTreeIndex)mParent.mChildren.get(new MarshalledKey(tmp.getKey()));
					}
					break;
				}
				prev = (BTreeIndex)node;
			}

			BTreeNode dstNode;
			if (next != null)
			{
				dstNode = next.getNode(0);
			}
			else
			{
				dstNode = prev.getNode(prev.mMap.size() - 1);
			}

			BTreeNode srcNode = mChildren.firstEntry().getValue();
			for (int i = 0; i < srcNode.mMap.size(); i++)
			{
				ArrayMapEntry tmp = new ArrayMapEntry();
				dstNode.mMap.put(srcNode.mMap.get(i, tmp), null);
			}

			mParent.mMap.remove(entry, null);
			mParent.mChildren.remove(new MarshalledKey(entry.getKey()));

//			BTreeTableImplementation.STOP = true;
		}

		return b;
	}


	BTreeNode getNode(int aIndex)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		mMap.get(aIndex, entry);

		BTreeNode node = mChildren.get(new MarshalledKey(entry.getKey()));

		if (node == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));
			node = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation, this) : new BTreeLeaf(mImplementation, this);
			node.mBlockPointer = bp;
			node.mMap = new ArrayMap(mImplementation.readBlock(bp));
		}

		return node;
	}


	@Override
	Object[] split()
	{
		System.out.println("split index");

		mImplementation.freeBlock(mBlockPointer);

		ArrayMap[] maps = mMap.split(mIndexSize);

		BTreeIndex a = new BTreeIndex(mImplementation, this);
		BTreeIndex b = new BTreeIndex(mImplementation, this);
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

		BTreeIndex a = new BTreeIndex(mImplementation, this);
		BTreeIndex b = new BTreeIndex(mImplementation, this);
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

		BTreeIndex newIndex = new BTreeIndex(mImplementation, this);
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);
		newIndex.mModified = true;

		return newIndex;
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

		mBlockPointer = mImplementation.writeBlock(mMap.array(), BlockType.INDEX);

		mModified = false;

		return true;
	}


	@Override
	public String toString()
	{
		return "BTreeIndex{" + "mChildren=" + mChildren + ", mMap=" + mMap + '}';
	}
}
