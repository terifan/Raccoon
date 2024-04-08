package org.terifan.raccoon.btree;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;


public class BTreeInteriorNode extends BTreeNode implements Iterable<ArrayMapEntry>
{
	TreeMap<ArrayMapEntry, BTreeNode> mChildren;
	ArrayMap mMap;


	BTreeInteriorNode(BTree aTree, BTreeInteriorNode aParent, int aLevel, ArrayMap aMap)
	{
		super(aTree, aParent, aLevel);

		mMap = aMap;
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	void get(ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKey(aEntry);
		loadNearestEntry(entry);
		getNode(entry).get(aEntry);
	}


	@Override
	void put(ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKey(aEntry);
		loadNearestEntry(entry);
		getNode(entry).put(aEntry);
		mModified = true;

		if (mMap.getCapacity() > mTree.getConfiguration().getNodeSize())
		{
			mTree.schedule(this);
		}
	}


	@Override
	void remove(ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry().setKey(aEntry);
		loadNearestEntry(entry);
		getNode(entry).remove(aEntry);
		mModified = true;

		if (mMap.getCapacity() < mTree.getConfiguration().getNodeSize() / 4)
		{
			mTree.schedule(this);
		}

		assert mMap.getFirst().getKeyType() == Type.FIRST : "First key expected to be empty: " + toString();
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapEntry aLowestKey, ArrayMapEntry aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeInteriorNode(this, aLowestKey, aHighestKey))
			{
				ArrayMapEntry lowestKey = aLowestKey;
				BTreeNode node = getNode(0);

				for (int i = 1, n = size(); i < n; i++)
				{
					BTreeNode nextNode = getNode(i);

					if (nextNode instanceof BTreeInteriorNode v)
					{
						ArrayMapEntry nextHigh = nextNode.size() == 1 ? aHighestKey : v.mMap.getKey(1);
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.mMap.getLast();
					}
					else if (nextNode instanceof BTreeLeafNode v)
					{
						ArrayMapEntry nextHigh = v.mMap.getLast();
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.mMap.getLast();
					}

					node = nextNode;
				}

				node.visit(aVisitor, lowestKey, aHighestKey);
			}

			aVisitor.afterInteriorNode(this);
		}
	}


	@Override
	boolean persist()
	{
		for (Entry<ArrayMapEntry, BTreeNode> entry : mChildren.entrySet())
		{
			BTreeNode node = entry.getValue();
			if (node.persist())
			{
				entry.getKey().setValueInstance(node.mBlockPointer);
				mMap.put(entry.getKey());
				mModified = true;
			}
		}

		if (!mModified)
		{
			assert mBlockPointer != null;
			return false;
		}

		RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
		RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

		mTree.freeBlock(mBlockPointer);
		mBlockPointer = mTree.writeBlock(mMap.array(), mLevel, BlockType.BTREE_NODE);
		mModified = false;

		return true;
	}


	<T extends BTreeNode> T getNode(int aIndex)
	{
		ArrayMapEntry entry = mMap.get(aIndex);

		BTreeNode node = getNode(entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(ArrayMapEntry aEntry)
	{
		BTreeNode childNode = mChildren.get(aEntry);

		if (childNode == null)
		{
			BlockPointer bp = BlockPointer.fromByteArray(aEntry.getValue());

			if (bp.getBlockType() == BlockType.BTREE_NODE)
			{
				childNode = new BTreeInteriorNode(mTree, this, mLevel - 1, new ArrayMap(mTree.readBlock(bp), mTree.getBlockSize()));
			}
			else
			{
				childNode = new BTreeLeafNode(mTree, this, new ArrayMap(mTree.readBlock(bp), mTree.getConfiguration().getLeafSize()));
			}

			childNode.mBlockPointer = bp;

			mChildren.put(aEntry, childNode);

			RuntimeDiagnostics.collectStatistics(bp.getBlockType() == BlockType.BTREE_NODE ? Operation.READ_NODE : Operation.READ_LEAF, 1);
		}

		return childNode;
	}


	synchronized BTreeNode __getNearestNode(ArrayMapEntry aEntry)
	{
		ArrayMapEntry nearestEntry = new ArrayMapEntry().setKey(aEntry.getKey(), aEntry.getKeyType());

		int index = loadNearestEntry(nearestEntry);

		BTreeNode nearestNode = getNode(nearestEntry);

		if (nearestNode.size() == 0)
		{
			System.out.println("*");
			mMap.loadKeyAndValue(index + 1, nearestEntry);
			nearestNode = getNode(nearestEntry);
		}

		return nearestNode;
	}


	@Override
	public String toString()
	{
		String s = String.format("BTreeInteriorNode{mLevel=%s, mMap=%s, mBuffer={", mLevel, mMap);
		for (ArrayMapEntry t : mChildren.keySet())
		{
			s += String.format("\"%s\",", t);
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	@Override
	protected String integrityCheck()
	{
		return mMap.integrityCheck();
	}


	@Override
	public int size()
	{
		return mMap.size();
	}


	private int loadNearestEntry(ArrayMapEntry aNearestEntry)
	{
		return mMap.loadNearestEntry(aNearestEntry);
	}


	private ArrayMap[] split(Integer aCapacity)
	{
		return mMap.splitHalf(aCapacity);
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		return mMap.iterator();
	}


	BTreeNode getChild(int aIndex)
	{
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
		// ?????????????????????
//		return (BTreeNode)mChildren.values().toArray()[aIndex];
		return getNode(aIndex);
	}


	int indexOf(BTreeNode aNode)
	{
		for (Entry<ArrayMapEntry, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getValue() == aNode)
			{
				return mMap.indexOf(entry.getKey());
			}
		}

		throw new IllegalStateException();
	}
}
