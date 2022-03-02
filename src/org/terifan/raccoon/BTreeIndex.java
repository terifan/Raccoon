package org.terifan.raccoon;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
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
			child.getValue().split();
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

//		ArrayMap[] maps = mMap.split();
//
//		ArrayMapEntry a = maps[1].get(0, new ArrayMapEntry());
//		ArrayMapEntry b = new ArrayMapEntry("".getBytes(), POINTER_PLACEHOLDER, (byte)0);
//
//		BTreeLeaf na = new BTreeLeaf();
//		BTreeLeaf nb = new BTreeLeaf();
//		na.mMap = maps[0];
//		nb.mMap = maps[1];
//
//		BTreeIndex newRoot = new BTreeIndex();
//		newRoot.mMap = new ArrayMap(mIndexSize);
//		newRoot.mMap.put(new ArrayMapEntry(a.getKey(), POINTER_PLACEHOLDER, (byte)0), null);
//		newRoot.mMap.put(new ArrayMapEntry(b.getKey(), POINTER_PLACEHOLDER, (byte)0), null);
//		newRoot.mChildren.put(new MarshalledKey(a.getKey()), na);
//		newRoot.mChildren.put(new MarshalledKey(b.getKey()), nb);
//
//		return newRoot;
	}
}
