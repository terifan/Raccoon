package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.BTreeTableImplementation.POINTER_PLACEHOLDER;
import static org.terifan.raccoon.BTreeTableImplementation.mIndexSize;
import org.terifan.raccoon.util.Result;


public class BTreeIndex extends BTreeNode
{
	TreeMap<MarshalledKey, BTreeNode> mChildren;


	BTreeIndex()
	{
		mChildren = new TreeMap<>();
	}


	boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, MarshalledKey aKey, Result<ArrayMapEntry> aResult)
	{
		Entry<MarshalledKey, BTreeNode> child = mChildren.ceilingEntry(aKey);

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

		if (child.getValue().put(this, aEntry, aKey, aResult))
		{
			System.out.println("+");

			BTreeNode[] split = child.getValue().split();

			mChildren.put(new MarshalledKey(split[0].mMap.getFirst().getKey()), split[0]);
			mChildren.put(new MarshalledKey(split[1].mMap.getFirst().getKey()), split[1]);

			mMap.insert(new ArrayMapEntry(split[0].mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0), null);
			return mMap.insert(new ArrayMapEntry(split[1].mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0), null);
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

		MarshalledKey keyB = new MarshalledKey(new byte[0]);
		MarshalledKey keyA = new MarshalledKey(midKeyBytes.getKey());

		BTreeIndex newIndex = new BTreeIndex();
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.key, POINTER_PLACEHOLDER, (byte)0), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.key, POINTER_PLACEHOLDER, (byte)0), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

		return newIndex;
	}
}
