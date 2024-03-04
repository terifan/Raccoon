package org.terifan.raccoon.btree;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTreeInteriorNode extends BTreeNode implements Iterable<ArrayMapEntry>
{
	TreeMap<ArrayMapKey, BTreeNode> mChildren;
	ArrayMap mMap;

//	protected int mDirtyLeafs;
//	protected int mSplitLeafs;


	BTreeInteriorNode(BTree aTree, BTreeInteriorNode aParent, int aLevel, ArrayMap aMap)
	{
		super(aTree, aParent, aLevel);

		mMap = aMap;
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	OpResult get(ArrayMapKey aKey)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);
		loadNearestEntry(entry);
		return getNode(entry).get(aKey);
	}


	@Override
	OpResult put(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
//		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		loadNearestEntry(nearestEntry);
		BTreeNode nearestNode = getNode(nearestEntry);
		OpResult result = nearestNode.put(aKey, aEntry);
//		if (mChange == null)
//		{
//			mChange = result.change;
//		}
		if (mMap.getCapacity() > mTree.getConfiguration().getNodeSize())
		{
			mTree.schedule(this);
		}

		return result;
	}


	@Override
	OpResult remove(ArrayMapKey aKey)
	{
//		mModified = true;

		int offset = mMap.nearestIndex(aKey);

		BTreeNode curntChld = getNode(offset);
		OpResult result = curntChld.remove(aKey);

		if (result.state == OpState.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mMap.getFirst().entry.getKey().get().toString().length() == 0 : "First key expected to be empty: " + toString();

		return result;
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeInteriorNode(this, aLowestKey, aHighestKey))
			{
//				mHighlight = BTree.RECORD_USE;

				ArrayMapKey lowestKey = aLowestKey;
				BTreeNode node = getNode(0);

				for (int i = 1, n = size(); i < n; i++)
				{
					BTreeNode nextNode = getNode(i);

					if (nextNode instanceof BTreeInteriorNode v)
					{
						ArrayMapKey nextHigh = nextNode.size() == 1 ? aHighestKey : v.mMap.getKey(1);
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.mMap.getLast().entry.getKey();
					}
					else if (nextNode instanceof BTreeLeafNode v)
					{
						ArrayMapKey nextHigh = v.mMap.getLast().entry.getKey();
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.mMap.getLast().entry.getKey();
					}

					node = nextNode;
				}

				node.visit(aVisitor, lowestKey, aHighestKey);
			}

			aVisitor.afterInteriorNode(this);
		}
	}


	@Override
	void commit()
	{
		assert assertValidCache() == null : assertValidCache();
		assert size() >= 2 : "interorior node has " + size() + " child";

		for (Entry<ArrayMapKey, BTreeNode> entry : mChildren.entrySet())
		{
			BTreeNode node = entry.getValue();

			node.commit();

//			if (node.commit())
//			{
				mMap.put(new ArrayMapEntry(entry.getKey(), node.mBlockPointer, TYPE_TREENODE));
//			}
		}

//		if (mModified)
//		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			mTree.freeBlock(mBlockPointer);

			mBlockPointer = mTree.writeBlock(mMap.array(), mLevel, BlockType.BTREE_NODE);
//		}

//		return mModified;
	}


	@Override
	protected void postCommit()
	{
//		if (mModified)
//		{
//			mModified = false;

			for (BTreeNode node : mChildren.values())
			{
				node.postCommit();
			}
//		}
//
//		mChildren.clear();
	}


	<T extends BTreeNode> T getNode(int aIndex)
	{
		OpResult op = mMap.get(aIndex);

		BTreeNode node = getNode(op.entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(ArrayMapEntry aEntry)
	{
		BTreeNode childNode = mChildren.get(aEntry.getKey());

		if (childNode == null)
		{
			BlockPointer bp = aEntry.getBlockPointer();

			if (bp.getBlockType() == BlockType.BTREE_NODE)
			{
				childNode = new BTreeInteriorNode(mTree, this, mLevel - 1, new ArrayMap(mTree.readBlock(bp), mTree.getBlockSize()));
			}
			else
			{
				childNode = new BTreeLeafNode(mTree, this, new ArrayMap(mTree.readBlock(bp), mTree.getConfiguration().getLeafSize()));
			}

			childNode.mBlockPointer = bp;

			mChildren.put(aEntry.getKey(), childNode);

			RuntimeDiagnostics.collectStatistics(bp.getBlockType() == BlockType.BTREE_NODE ? Operation.READ_NODE : Operation.READ_LEAF, 1);
		}

		return childNode;
	}


	synchronized BTreeNode __getNearestNode(ArrayMapEntry aEntry)
	{
		ArrayMapEntry nearestEntry = new ArrayMapEntry(aEntry.getKey());

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
		String s = String.format("BTreeInteriorNode{"+UNIQUE+", mLevel=%s, mMap=%s, mBuffer={", mLevel, mMap);
		for (ArrayMapKey t : mChildren.keySet())
		{
			s += String.format("\"%s\",", t);
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	private String assertValidCache()
	{
		for (ArrayMapKey key : mChildren.keySet())
		{
			OpResult result = mMap.get(key);
			if (result.state != OpState.MATCH)
			{
				return "key not found: " + key;
			}
		}
		return null;
	}


//	void put(ArrayMapKey aKey, BTreeNode aNode)
//	{
//		assert aNode.mParent == this;
//		mChildren.put(aKey, aNode);
//	}
//
//
//	private void _remove(ArrayMapKey aKey)
//	{
//		mChildren.remove(aKey);
//		mMap.remove(aKey);
//	}
//
//
//	private void remove(int aIndex)
//	{
//		OpResult op = mMap.get(aIndex);
//		mChildren.remove(op.entry.getKey());
//		mMap.remove(aIndex);
//	}
//
//
//	private void clear()
//	{
//		mChildren.clear();
//	}
//
//
//	void putEntry(ArrayMapEntry aArrayMapEntry)
//	{
//		mMap.put(aArrayMapEntry);
//	}


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
		for (Entry<ArrayMapKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getValue() == aNode)
			{
				return mMap.indexOf(entry.getKey());
			}
		}

		throw new IllegalStateException();
	}
}
