package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.util.Log;
import org.terifan.raccoon.util.Result;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex()
	{
		mChildren = new TreeMap<>();
	}


	boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		Entry<MarshalledKey, BTreeNode> child = mChildren.higherEntry(MarshalledKey.unmarshall(aEntry.getKey()));

		System.out.println(mChildren.size());
		System.out.println(mChildren.firstKey());
		System.out.println(mChildren.lastKey());
		System.out.println(MarshalledKey.unmarshall(aEntry.getKey()));
		System.out.println(child);

//		if (child == null)
//		{
//			int index = aParent.mMap.indexOf(aEntry.getKey());
//
//			ArrayMapEntry entry = new ArrayMapEntry();
//			((BTreeIndex)aParent).mMap.get(index, entry);
//
//			BlockPointer bp = new BlockPointer().unmarshal(ByteArrayBuffer.wrap(aKey.key));
//
//			child = BTreeNode.newNode(bp);
//			child.mMap = new ArrayMap(readBlock(bp));
//		}

		if (child.getValue().put(this, aEntry, aResult))
		{
			System.out.println("+");

			BTreeNode[] split = child.getValue().split();

			mChildren.put(MarshalledKey.unmarshall(split[1].mMap.getFirst().getKey()), split[0]);
			mChildren.put(child.getKey(), split[1]);

			mMap.insert(new ArrayMapEntry(split[1].mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x88), null);
			mMap.insert(new ArrayMapEntry(child.getKey().marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
		}

		return false;
	}


	@Override
	BTreeNode[] split()
	{
		ArrayMap[] maps = mMap.split();

		BTreeIndex a = new BTreeIndex();
		BTreeIndex b = new BTreeIndex();
		a.mMap = maps[0];
		b.mMap = maps[1];

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
		ArrayMap[] maps = mMap.split();

		BTreeIndex a = new BTreeIndex();
		BTreeIndex b = new BTreeIndex();
		a.mMap = maps[0];
		b.mMap = maps[1];

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

		MarshalledKey keyA = MarshalledKey.unmarshall(midKeyBytes.getKey());
		MarshalledKey keyB = new MarshalledKey(true);

		BTreeIndex newIndex = new BTreeIndex();
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

		return newIndex;
	}
}
