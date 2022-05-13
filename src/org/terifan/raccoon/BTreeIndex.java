package org.terifan.raccoon;

import java.util.Arrays;
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
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(ArrayMapEntry aEntry)
	{
//		Entry<MarshalledKey, BTreeNode> child = mChildren.higherEntry(MarshalledKey.unmarshall(aEntry.getKey()));

//		System.out.println("index " + aEntry + " " + mChildren.keySet());

		MarshalledKey putKey = MarshalledKey.unmarshall(aEntry.getKey());
		MarshalledKey childKey = null;
		MarshalledKey lastKey = null;

		for (MarshalledKey compKey : mChildren.keySet())
		{
			lastKey = compKey;
			if (putKey.compareTo(compKey) < 0)
			{
				break;
			}
			childKey = compKey;
		}
		if (childKey == null)
		{
			childKey = lastKey;
		}

		BTreeNode childNode = mChildren.get(childKey);

		return childNode.get(aEntry);
	}


	@Override
	boolean put(BTreeIndex aParent, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
//		Entry<MarshalledKey, BTreeNode> child = mChildren.higherEntry(MarshalledKey.unmarshall(aEntry.getKey()));

		MarshalledKey putKey = MarshalledKey.unmarshall(aEntry.getKey());
		MarshalledKey childKey = null;
		MarshalledKey lastKey = null;

		for (MarshalledKey compKey : mChildren.keySet())
		{
			lastKey = compKey;
			if (putKey.compareTo(compKey) < 0)
			{
				break;
			}
			childKey = compKey;
		}
		if (childKey == null)
		{
			childKey = lastKey;
		}

		BTreeNode childNode = mChildren.get(childKey);

		if (childNode.put(this, aEntry, aResult))
		{
			BTreeNode[] split = childNode.split();

//			System.out.println("--------" + split[0].mMap);
//			System.out.println("--------" + split[1].mMap);

			mChildren.remove(childKey);
			mMap.remove(new ArrayMapEntry(childKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			mChildren.put(MarshalledKey.unmarshall(split[0].mMap.getFirst().getKey()), split[0]);
			mChildren.put(MarshalledKey.unmarshall(split[1].mMap.getFirst().getKey()), split[1]);

			boolean overflow = !mMap.insert(new ArrayMapEntry(split[0].mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			overflow |= !mMap.insert(new ArrayMapEntry(split[1].mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

			return overflow;
		}
		else
		{
			if (Arrays.compare(aEntry.getKey(), mMap.getFirst().getKey()) < 0)
			{
				mChildren.remove(childKey);
				mMap.remove(new ArrayMapEntry(childKey.marshall(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);

				mChildren.put(MarshalledKey.unmarshall(childNode.mMap.getFirst().getKey()), childNode);

				mMap.insert(new ArrayMapEntry(childNode.mMap.getFirst().getKey(), BTreeTableImplementation.POINTER_PLACEHOLDER, (byte)0x44), null);
			}
		}

		return false;
	}


	@Override
	BTreeNode[] split()
	{
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mIndexSize);

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
		ArrayMap[] maps = mMap.split(BTreeTableImplementation.mIndexSize);

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

		MarshalledKey keyA = MarshalledKey.unmarshall(a.mMap.getFirst().getKey());
		MarshalledKey keyB = MarshalledKey.unmarshall(midKeyBytes.getKey());

		BTreeIndex newIndex = new BTreeIndex();
		newIndex.mMap = new ArrayMap(mIndexSize);
		newIndex.mMap.put(new ArrayMapEntry(keyA.marshall(), POINTER_PLACEHOLDER, (byte)0x99), null);
		newIndex.mMap.put(new ArrayMapEntry(keyB.marshall(), POINTER_PLACEHOLDER, (byte)0x22), null);
		newIndex.mChildren.put(keyA, a);
		newIndex.mChildren.put(keyB, b);

		return newIndex;
	}
}
