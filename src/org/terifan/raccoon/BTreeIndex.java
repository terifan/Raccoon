package org.terifan.raccoon;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.storage.BlockPointer;
import org.terifan.raccoon.util.ByteArrayBuffer;
import org.terifan.raccoon.util.Result;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex(BTreeTableImplementation aImplementation)
	{
		super(aImplementation);
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(MarshalledKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestNG(entry);

		MarshalledKey nearestKey = MarshalledKey.unmarshall(entry.getKey());

		BTreeNode nearestNode = mChildren.get(nearestKey);

		if (nearestNode == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation) : new BTreeLeaf(mImplementation);
			nearestNode.mBlockPointer = bp;
			nearestNode.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(nearestKey, nearestNode);
		}

		return nearestNode.get(aKey, aEntry);
	}


	@Override
	boolean put(BTreeIndex aParent, MarshalledKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified =  true;

		ArrayMapEntry entry = new ArrayMapEntry(aKey.marshall());
		mMap.nearestNG(entry);

		MarshalledKey nearestKey = MarshalledKey.unmarshall(entry.getKey());

		BTreeNode nearestNode = mChildren.get(nearestKey);

		if (nearestNode == null)
		{
			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(entry.getValue()));

			nearestNode = bp.getBlockType() == BlockType.INDEX ? new BTreeIndex(mImplementation) : new BTreeLeaf(mImplementation);
			nearestNode.mBlockPointer = bp;
			nearestNode.mMap = new ArrayMap(mImplementation.readBlock(bp));

			mChildren.put(nearestKey, nearestNode);
		}

		if (nearestNode.put(this, aKey, aEntry, aResult))
		{
			mImplementation.freeBlock(nearestNode.mBlockPointer);

			BTreeNode[] split = nearestNode.split();

			mMap.remove(new ArrayMapEntry(nearestKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			byte[] rightKey = split[1].mMap.getFirst().getKey();

			mChildren.put(nearestKey, split[0]);
			mChildren.put(MarshalledKey.unmarshall(rightKey), split[1]);

			boolean overflow = !mMap.insert(new ArrayMapEntry(nearestKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			overflow |= !mMap.insert(new ArrayMapEntry(rightKey, BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			return overflow;
		}
		else
		{
			if (Arrays.compare(aEntry.getKey(), mMap.getFirst().getKey()) < 0)
			{
				mChildren.remove(aKey);
				mMap.remove(new ArrayMapEntry(aKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

				mChildren.put(MarshalledKey.unmarshall(nearestNode.mMap.getFirst().getKey()), nearestNode);

				mMap.insert(new ArrayMapEntry(nearestNode.mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			}
		}

		return false;
	}


	@Override
	BTreeNode[] split()
	{
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mIndexSize);

		BTreeIndex a = new BTreeIndex(mImplementation);
		BTreeIndex b = new BTreeIndex(mImplementation);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
		maps[1].get(0, midKeyBytes);
		MarshalledKey midKey = MarshalledKey.unmarshall(midKeyBytes.getKey());

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

		return new BTreeNode[]
		{
			a, b
		};
	}


	BTreeNode grow()
	{
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mIndexSize);

		BTreeIndex a = new BTreeIndex(mImplementation);
		BTreeIndex b = new BTreeIndex(mImplementation);
		a.mMap = maps[0];
		b.mMap = maps[1];
		a.mModified = true;
		b.mModified = true;

		ArrayMapEntry midKeyBytes = new ArrayMapEntry();
		b.mMap.get(0, midKeyBytes);

		MarshalledKey midKey = MarshalledKey.unmarshall(midKeyBytes.getKey());

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

		MarshalledKey keyA = MarshalledKey.unmarshall(a.mMap.getFirst().getKey());
		MarshalledKey keyB = MarshalledKey.unmarshall(midKeyBytes.getKey());

		BTreeIndex newIndex = new BTreeIndex(mImplementation);
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

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
		}

		mChildren.clear();

		if (!mModified)
		{
			return false;
		}

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
