package org.terifan.raccoon.btree;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import static org.terifan.raccoon.btree.BTree.BLOCKPOINTER_PLACEHOLDER;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.btree.ArrayMap.PutResult;
import org.terifan.raccoon.RuntimeDiagnostics;
import org.terifan.raccoon.RuntimeDiagnostics.Operation;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTreeInteriorNode extends BTreeNode implements Iterable<ArrayMapEntry>
{
	TreeMap<ArrayMapKey, BTreeNode> mChildren;
	ArrayMap mArrayMap;


	BTreeInteriorNode(BTree aTree, BTreeInteriorNode aParent, int aLevel, ArrayMap aMap)
	{
		super(aTree, aParent, aLevel);

		mArrayMap = aMap;
		mChildren = new TreeMap<>((o1, o2) -> o1.compareTo(o2));
	}


	@Override
	boolean get(ArrayMapKey aKey, ArrayMapEntry aEntry)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);

		loadNearestEntry(entry);

		BTreeNode node = getNode(entry);

		return node.get(aKey, aEntry);
	}


	@Override
	PutResult put(ArrayMapKey aKey, ArrayMapEntry aEntry, Result<ArrayMapEntry> aResult)
	{
		mModified = true;

		ArrayMapEntry nearestEntry = new ArrayMapEntry(aKey);
		loadNearestEntry(nearestEntry);
		BTreeNode nearestNode = getNode(nearestEntry);

		if (mLevel == 1 ? ((BTreeLeafNode)nearestNode).mMap.getCapacity() > mTree.getLeafSize() || ((BTreeLeafNode)nearestNode).mMap.getFreeSpace() < aEntry.getMarshalledLength() : ((BTreeInteriorNode)nearestNode).getUsedSpace() > mTree.getNodeSize())
		{
			ArrayMapKey leftKey = nearestEntry.getKey();

			remove(leftKey);

			SplitResult split = nearestNode.split();

			ArrayMapKey rightKey = split.getRightKey();

			put(leftKey, split.getLeftNode());
			put(rightKey, split.getRightNode());

			insertEntry(new ArrayMapEntry(leftKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			insertEntry(new ArrayMapEntry(rightKey, BTree.BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));

			nearestEntry = new ArrayMapEntry(aKey);
			loadNearestEntry(nearestEntry);
			nearestNode = getNode(nearestEntry);

			assert size() >= 2;
		}

		return nearestNode.put(aKey, aEntry, aResult);
	}


	@Override
	RemoveResult remove(ArrayMapKey aKey, Result<ArrayMapEntry> aOldEntry)
	{
		mModified = true;

		int offset = nearestIndex(aKey);

		BTreeNode curntChld = getNode(offset);
		BTreeNode leftChild = offset == 0 ? null : getNode(offset - 1);
		BTreeNode rghtChild = offset + 1 == size() ? null : getNode(offset + 1);

		assert ((curntChld == null || curntChld instanceof BTreeInteriorNode) && (leftChild == null || leftChild instanceof BTreeInteriorNode) && (rghtChild == null || rghtChild instanceof BTreeInteriorNode)) || ((curntChld == null || curntChld instanceof BTreeLeafNode) && (leftChild == null || leftChild instanceof BTreeLeafNode) && (rghtChild == null || rghtChild instanceof BTreeLeafNode));

		int keyLimit = mLevel == 1 ? 0 : 1;
		int sizeLimit = mLevel == 1 ? mTree.getLeafSize() : mTree.getNodeSize();

		if (leftChild != null && (curntChld.size() + leftChild.size()) < sizeLimit || rghtChild != null && (curntChld.size() + rghtChild.size()) < sizeLimit)
		{
			offset = mergeNodes(offset, curntChld, leftChild, rghtChild, keyLimit, sizeLimit);
		}

		RemoveResult result = curntChld.remove(aKey, aOldEntry);

		if (curntChld.mLevel == 0 && ((BTreeLeafNode)curntChld).mMap.size() == 0)
		{
			if (offset == 0)
			{
				remove(ArrayMapKey.EMPTY);

				ArrayMapEntry firstEntry = mArrayMap.getFirst();

				BTreeNode firstNode = getNode(firstEntry);

				remove(firstEntry.getKey());

				firstEntry.setKey(ArrayMapKey.EMPTY);

				insertEntry(firstEntry, firstNode);
			}
			else
			{
				ArrayMapEntry targetEntry = getEntry(offset, new ArrayMapEntry());

				remove(targetEntry.getKey());
			}

			mTree.freeBlock(curntChld.mBlockPointer);
		}

		if (result == RemoveResult.NO_MATCH)
		{
			return result;
		}

		assert assertValidCache() == null : assertValidCache();
		assert mArrayMap.getFirst().getKey().get().toString().length() == 0 : "First key expected to be empty: " + toString();

		return result;
	}


	@Override
	void visit(BTreeVisitor aVisitor, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
	{
		if (aVisitor.beforeAnyNode(this))
		{
			if (aVisitor.beforeInteriorNode(this, aLowestKey, aHighestKey))
			{
				mHighlight = BTree.RECORD_USE;

				ArrayMapKey lowestKey = aLowestKey;
				BTreeNode node = getNode(0);

				for (int i = 1, n = size(); i < n; i++)
				{
					BTreeNode nextNode = getNode(i);

					if (nextNode instanceof BTreeInteriorNode v)
					{
						ArrayMapKey nextHigh = nextNode.size() == 1 ? aHighestKey : v.getKey(1);
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.getLast().getKey();
					}
					else if (nextNode instanceof BTreeLeafNode v)
					{
						ArrayMapKey nextHigh = v.mMap.getLast().getKey();
						node.visit(aVisitor, lowestKey, nextHigh);
						lowestKey = v.mMap.getLast().getKey();
					}

					node = nextNode;
				}

				node.visit(aVisitor, lowestKey, aHighestKey);
			}

			aVisitor.afterInteriorNode(this);
		}
	}


	private int mergeNodes(int aOffset, BTreeNode aCurntChld, BTreeNode aLeftChild, BTreeNode aRghtChild, int aKeyLimit, int aSizeLimit)
	{
		int newOffset = aOffset;

		boolean a = aLeftChild != null;
		boolean b = aRghtChild != null;

		if (aCurntChld instanceof BTreeInteriorNode)
		{
			if (a)
			{
				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aLeftChild).getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aRghtChild).getUsedSpace() <= aSizeLimit;
			}

			if (a && b)
			{
				if (((BTreeInteriorNode)aLeftChild).getFreeSpace() < ((BTreeInteriorNode)aRghtChild).getFreeSpace())
				{
					a = false;
				}
				else
				{
					b = false;
				}
			}
		}
		else
		{
			if (a)
			{
				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aLeftChild).mMap.getUsedSpace() <= aSizeLimit;
			}
			if (b)
			{
				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeLeafNode)aCurntChld).mMap.getUsedSpace() + ((BTreeLeafNode)aRghtChild).mMap.getUsedSpace() <= aSizeLimit;
			}

			if (a && b)
			{
				if (((BTreeLeafNode)aLeftChild).mMap.getFreeSpace() < ((BTreeLeafNode)aRghtChild).mMap.getFreeSpace())
				{
					a = false;
				}
				else
				{
					b = false;
				}
			}
		}

		if (mLevel == 1)
		{
			if (a)
			{
				mergeLeafs(newOffset - 1, (BTreeLeafNode)aLeftChild, (BTreeLeafNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeLeafs(newOffset + 1, (BTreeLeafNode)aRghtChild, (BTreeLeafNode)aCurntChld);
			}
			if (newOffset <= 0)
			{
				clearFirstKey();
			}
		}
		else
		{
			if (a)
			{
				mergeIndices(newOffset - 1, (BTreeInteriorNode)aLeftChild, (BTreeInteriorNode)aCurntChld);
				newOffset--;
			}
			else if (b)
			{
				mergeIndices(newOffset + 1, (BTreeInteriorNode)aRghtChild, (BTreeInteriorNode)aCurntChld);
			}
		}

		// update lowest key after a merge
		if (newOffset > 0)
		{
			ArrayMapEntry nearestEntry = getEntry(newOffset, new ArrayMapEntry());
			ArrayMapKey nearestKey = nearestEntry.getKey();

			ArrayMapKey firstKey = findLowestLeafKey(aCurntChld);

			if (!firstKey.equals(nearestEntry.getKey()))
			{
				nearestEntry.setKey(firstKey);

				mArrayMap.remove(newOffset, null);
				insertEntry(nearestEntry);

				BTreeNode childNode = removeXX(nearestKey);
				if (childNode != null)
				{
					put(firstKey, childNode);
				}
			}

			assert size() >= 2;
		}

		return newOffset;
	}


	@Override
	SplitResult split()
	{
		mTree.freeBlock(mBlockPointer);

		ArrayMap[] maps = split(mTree.getNodeSize());

		BTreeInteriorNode left = new BTreeInteriorNode(mTree, mParent, mLevel, maps[0]);
		BTreeInteriorNode right = new BTreeInteriorNode(mTree, mParent, mLevel, maps[1]);
		left.mModified = true;
		right.mModified = true;

		ArrayMapKey midKey = right.getKey(0);

		for (Entry<ArrayMapKey, BTreeNode> childEntry : entrySet())
		{
			BTreeNode childNode = childEntry.getValue();
			ArrayMapKey key = childEntry.getKey();
			if (key.compareTo(midKey) < 0)
			{
				childNode.mParent = left;
				left.put(key, childNode);
			}
			else
			{
				childNode.mParent = right;
				right.put(key, childNode);
			}
		}

		ArrayMapEntry firstRight = right.mArrayMap.getFirst();

		ArrayMapKey keyLeft = ArrayMapKey.EMPTY;
		ArrayMapKey keyRight = firstRight.getKey();

		BTreeNode firstChild = right.getNode(firstRight);

		right.remove(keyRight);

		firstRight.setKey(keyLeft);

		right.insertEntry(firstRight);
		right.put(keyLeft, firstChild);

		assert left.size() >= 2;
		assert right.size() >= 2;

		return new SplitResult(left, right, keyLeft, keyRight);
	}


	BTreeNode grow()
	{
		SplitResult split = split();

		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mParent, mLevel + 1, new ArrayMap(mTree.getNodeSize()));
		interior.mModified = true;
		interior.insertEntry(new ArrayMapEntry(split.getLeftKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		interior.insertEntry(new ArrayMapEntry(split.getRightKey(), BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		split.getLeftNode().mParent = interior;
		split.getRightNode().mParent = interior;
		interior.put(split.getLeftKey(), split.getLeftNode());
		interior.put(split.getRightKey(), split.getRightNode());

		clear();

		return interior;
	}


	/**
	 * Shrinks the tree by removing this node and merging all child nodes into a single index node which is returned.
	 */
	BTreeInteriorNode shrink()
	{
		BTreeInteriorNode interior = new BTreeInteriorNode(mTree, mParent, mLevel - 1, new ArrayMap(mTree.getNodeSize()));
		interior.mModified = true;

		for (int i = 0; i < size(); i++)
		{
			BTreeInteriorNode node = getNode(i);

			boolean first = i > 0;
			for (ArrayMapEntry entry : node)
			{
				ArrayMapEntry newEntry;
				if (first)
				{
					newEntry = new ArrayMapEntry(getKey(i), entry);
					first = false;
				}
				else
				{
					newEntry = entry;
				}

				BTreeNode childNode = node.get(entry.getKey());
				childNode.mParent = interior;
				interior.insertEntry(newEntry, childNode);
			}

			mTree.freeBlock(node.mBlockPointer);
		}

		mTree.freeBlock(mBlockPointer);

		return interior;
	}


	private void mergeIndices(int aFromIndex, BTreeInteriorNode aFrom, BTreeInteriorNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		fixFirstKey(aFrom);
		fixFirstKey(aTo);

		for (int i = 0, sz = aFrom.size(); i < sz; i++)
		{
			aFrom.getEntry(i, temp);

			BTreeNode node = aFrom.getNode(temp);
			node.mParent = aTo;

			aTo.insertEntry(temp);
			aTo.put(temp.getKey(), node);
		}

		aFrom.clearEntries();
		aFrom.clear();
		mTree.freeBlock(aFrom.mBlockPointer);

		aTo.clearFirstKey();

		remove(aFromIndex);

		if (aFromIndex == 0)
		{
			clearFirstKey();
		}

		aTo.mModified = true;
	}


	private void mergeLeafs(int aIndex, BTreeLeafNode aFrom, BTreeLeafNode aTo)
	{
		ArrayMapEntry temp = new ArrayMapEntry();

		for (int i = 0, sz = aFrom.size(); i < sz; i++)
		{
			aFrom.mMap.get(i, temp);
			aTo.mMap.insert(temp);
		}

		aFrom.mMap.clear();
		mTree.freeBlock(aFrom.mBlockPointer);

		remove(aIndex);

		aTo.mModified = true;
	}


	private void fixFirstKey(BTreeInteriorNode aNode)
	{
		ArrayMapEntry firstEntry = aNode.mArrayMap.getFirst();

		assert firstEntry.getKey().get().toString().length() == 0 : firstEntry.getKey();

		BTreeNode firstNode = aNode.getNode(firstEntry);

		firstEntry.setKey(findLowestLeafKey(aNode));

		aNode.remove(ArrayMapKey.EMPTY);

		aNode.insertEntry(firstEntry);
		aNode.put(firstEntry.getKey(), firstNode);
	}


	private void clearFirstKey()
	{
		ArrayMapEntry firstEntry = mArrayMap.getFirst();

		if (firstEntry.getKey().size() > 0)
		{
			BTreeNode childNode = removeFirst();

			firstEntry.setKey(ArrayMapKey.EMPTY);

			insertEntry(firstEntry);

			if (childNode != null)
			{
				put(ArrayMapKey.EMPTY, childNode);
			}
		}
	}


	private static ArrayMapKey findLowestLeafKey(BTreeNode aNode)
	{
		if (aNode instanceof BTreeInteriorNode v)
		{
			for (int i = 0; i < v.size(); i++)
			{
				ArrayMapKey b = findLowestLeafKey(v.getNode(i));
				if (b.size() > 0)
				{
					return b;
				}
			}
			throw new IllegalStateException();
		}

		BTreeLeafNode leaf = (BTreeLeafNode)aNode;

		if (leaf.mMap.isEmpty())
		{
			return ArrayMapKey.EMPTY;
		}

		return leaf.mMap.getKey(0);
	}


	/**
	 * Merge entries in all child nodes into a single LeafNode which is returned.
	 */
	BTreeLeafNode downgrade()
	{
		assert mLevel == 1;

		BTreeLeafNode newLeaf = getNode(0);
		newLeaf.mModified = true;

		for (int i = 1; i < size(); i++)
		{
			BTreeLeafNode node = getNode(i);

			node.mMap.forEach(e -> newLeaf.mMap.insert(e));

			mTree.freeBlock(node.mBlockPointer);
		}

		mTree.freeBlock(mBlockPointer);

		return newLeaf;
	}


	@Override
	boolean commit()
	{
		assert assertValidCache() == null : assertValidCache();
		assert size() >= 2;

		for (Entry<ArrayMapKey, BTreeNode> entry : entrySet())
		{
			BTreeNode node = entry.getValue();

			if (node.commit())
			{
				insertEntry(new ArrayMapEntry(entry.getKey(), node.mBlockPointer, TYPE_TREENODE));
			}
		}

		if (mModified)
		{
			RuntimeDiagnostics.collectStatistics(Operation.FREE_NODE, mBlockPointer);
			RuntimeDiagnostics.collectStatistics(Operation.WRITE_NODE, 1);

			mTree.freeBlock(mBlockPointer);

			mBlockPointer = mTree.writeBlock(array(), mLevel, BlockType.BTREE_NODE);
		}

		return mModified;
	}


	@Override
	protected void postCommit()
	{
		if (mModified)
		{
			mModified = false;

			for (BTreeNode node : mChildren.values())
			{
				node.postCommit();
			}
		}

		clear();
	}


	<T extends BTreeNode> T getNode(int aOffset)
	{
		ArrayMapEntry entry = new ArrayMapEntry();

		getEntry(aOffset, entry);

		BTreeNode node = getNode(entry);

		return (T)node;
	}


	synchronized BTreeNode getNode(ArrayMapEntry aEntry)
	{
		BTreeNode childNode = get(aEntry.getKey());

		if (childNode == null)
		{
			BlockPointer bp = aEntry.getBlockPointer();

			if (bp.getBlockType() == BlockType.BTREE_NODE)
			{
				childNode = new BTreeInteriorNode(mTree, this, mLevel - 1, new ArrayMap(mTree.readBlock(bp)));
			}
			else
			{
				childNode = new BTreeLeafNode(mTree, this, new ArrayMap(mTree.readBlock(bp)));
			}

			childNode.mBlockPointer = bp;

			put(aEntry.getKey(), childNode);

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
			mArrayMap.loadKeyAndValue(index + 1, nearestEntry);
			nearestNode = getNode(nearestEntry);
		}

		return nearestNode;
	}


	@Override
	public String toString()
	{
		String s = String.format("BTreeInteriorNode{mLevel=%s, mMap=%s, mBuffer={", mLevel, this);
		for (ArrayMapKey t : keySet())
		{
			s += String.format("\"%s\",", t);
		}
		return s.substring(0, s.length() - 1) + '}';
	}


	private String assertValidCache()
	{
		for (ArrayMapKey key : keySet())
		{
			ArrayMapEntry entry = new ArrayMapEntry(key);
			if (!getEntry(entry))
			{
				return entry.toString();
			}
		}
		return null;
	}


	@Override
	protected void scan(ScanResult aScanResult)
	{
		int fillRatio = getUsedSpace() * 100 / mTree.getNodeSize();
		aScanResult.log.append("{" + (mBlockPointer == null ? "" : mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");

		boolean first = true;
		aScanResult.log.append("'");
		for (ArrayMapEntry entry : this)
		{
			if (!first)
			{
				aScanResult.log.append(":");
			}
			first = false;
			String s = stringifyKey(entry.getKey());
			aScanResult.log.append(s.isEmpty() ? "*" : s);
		}
		aScanResult.log.append("'");

		if (mHighlight)
		{
			aScanResult.log.append("#a00#a00#fff");
		}
		else if (size() == 1)
		{
			aScanResult.log.append("#000#ff0#000");
		}
		else if (fillRatio > 100)
		{
			aScanResult.log.append(mModified ? "#a00#a00#fff" : "#666#666#fff");
		}
		else
		{
			aScanResult.log.append(mModified ? "#f00#f00#fff" : "#888#fff#000");
		}

		first = true;
		aScanResult.log.append("[");

		for (int i = 0, sz = size(); i < sz; i++)
		{
			if (!first)
			{
				aScanResult.log.append(",");
			}
			first = false;

			BTreeNode child = getNode(i);

			child.scan(aScanResult);
		}

		aScanResult.log.append("]");
	}





	private BTreeNode get(ArrayMapKey aKey)
	{
		return mChildren.get(aKey);
	}


	void put(ArrayMapKey aKey, BTreeNode aNode)
	{
		assert aNode.mParent == this;
		mChildren.put(aKey, aNode);
	}


	private BTreeNode removeXX(ArrayMapKey aKey)
	{
		return mChildren.remove(aKey);
	}


	private void remove(ArrayMapKey aKey)
	{
		mChildren.remove(aKey);
		mArrayMap.remove(aKey, null);
	}


	private void remove(int aIndex)
	{
		ArrayMapEntry temp = new ArrayMapEntry();
		getEntry(aIndex, temp);
		mChildren.remove(temp.getKey());
		mArrayMap.remove(aIndex, null);
	}


	private void clear()
	{
		mChildren.clear();
	}


	private Iterable<Entry<ArrayMapKey, BTreeNode>> entrySet()
	{
		return mChildren.entrySet();
	}


	private Iterable<ArrayMapKey> keySet()
	{
		return mChildren.keySet();
	}


	int getUsedSpace()
	{
		return mArrayMap.getUsedSpace();
	}


	private ArrayMapEntry getEntry(int aIndex, ArrayMapEntry aEntry)
	{
		return mArrayMap.get(aIndex, aEntry);
	}


	private boolean getEntry(ArrayMapEntry aEntry)
	{
		return mArrayMap.get(aEntry);
	}


	void putEntry(ArrayMapEntry aArrayMapEntry)
	{
		mArrayMap.put(aArrayMapEntry, null);
	}


	private void insertEntry(ArrayMapEntry aArrayMapEntry)
	{
		mArrayMap.insert(aArrayMapEntry);
	}


	private void insertEntry(ArrayMapEntry aArrayMapEntry, BTreeNode aChildNode)
	{
		assert aChildNode.mParent == this;
		mArrayMap.insert(aArrayMapEntry);
		mChildren.put(aArrayMapEntry.getKey(), aChildNode);
	}


	private int getFreeSpace()
	{
		return mArrayMap.getFreeSpace();
	}


	private ArrayMapKey getKey(int aIndex)
	{
		return mArrayMap.getKey(aIndex);
	}


	private BTreeNode removeFirst()
	{
		Entry<ArrayMapKey, BTreeNode> tmp = mChildren.firstEntry();
		mArrayMap.removeFirst();
		mChildren.remove(tmp.getKey());
		return tmp.getValue();
	}


	private ArrayMapEntry getLast()
	{
		return mArrayMap.getLast();
	}


	private void clearEntries()
	{
		mArrayMap.clear();
	}


	@Override
	protected String integrityCheck()
	{
		return mArrayMap.integrityCheck();
	}


	@Override
	public int size()
	{
		return mArrayMap.size();
	}


	private byte[] array()
	{
		return mArrayMap.array();
	}


	private int nearestIndex(ArrayMapKey aKey)
	{
		return mArrayMap.nearestIndex(aKey);
	}


	private int loadNearestEntry(ArrayMapEntry aNearestEntry)
	{
		return mArrayMap.loadNearestEntry(aNearestEntry);
	}


	private ArrayMap[] split(Integer aCapacity)
	{
		return mArrayMap.split(aCapacity);
	}


	@Override
	public Iterator<ArrayMapEntry> iterator()
	{
		return mArrayMap.iterator();
	}


	BTreeNode getChild(int aIndex)
	{
		return (BTreeNode)mChildren.values().toArray()[aIndex];
	}


	int indexOf(BTreeNode aNode)
	{
		for (Entry<ArrayMapKey, BTreeNode> entry : mChildren.entrySet())
		{
			if (entry.getValue() == aNode)
			{
				return mArrayMap.indexOf(entry.getKey());
			}
		}

		throw new IllegalStateException();
	}
}
