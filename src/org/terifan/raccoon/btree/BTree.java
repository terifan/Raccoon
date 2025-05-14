package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.btree.ArrayMapEntry.Type;
import org.terifan.raccoon.util.Console;


public class BTree implements AutoCloseable
{
	private final static Logger log = Logger.getLogger();

	private final static byte[] BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL).toByteArray();

	public static boolean RECORD_USE;

	private final BTreeConfiguration mConfiguration;
	private BlockAccessor mBlockAccessor;
	private BTreeNode mRoot;
	private long mModCount;
	private long mUpdateCounter;

	private final ArrayList<HashSet<BTreeNode>> mSchedule = new ArrayList<>()
	{
		{
			for (int i = 0; i < 10; i++)
			{
				add(new HashSet<>());
			}
		}
	};


	public BTree(BlockAccessor aBlockAccessor, BTreeConfiguration aConfiguration)
	{
		assert aBlockAccessor != null;

		mBlockAccessor = aBlockAccessor;
		mConfiguration = aConfiguration;

		initialize();
	}


	private void initialize()
	{
		if (mConfiguration.getRoot() != null)
		{
			log.i("open table");
			log.inc();
			BlockPointer pointer = mConfiguration.getRoot();
			mRoot = pointer.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(this, null, pointer.getBlockLevel(), new ArrayMap(readBlock(pointer), getBlockSize())) : new BTreeLeafNode(this, null, new ArrayMap(readBlock(pointer), getBlockSize()));
			mRoot.mBlockPointer = pointer;
			log.dec();
		}
		else
		{
			log.i("create table");
			log.inc();
			mRoot = new BTreeLeafNode(this, null, new ArrayMap(mConfiguration.getLeafSize(), getBlockSize()));
			log.dec();
		}
	}


	/**
	 * Return the configuration for this BTree required to open it.
	 *
	 * @return the same instance provided in the constructor.
	 */
	public BTreeConfiguration getConfiguration()
	{
		return mConfiguration;
	}


	public void get(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		mRoot.get(aEntry);
	}


	public void put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.getMarshalledLength() > mConfiguration.getLimitEntrySize())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: length: " + aEntry.getMarshalledLength() + ", maximum: " + mConfiguration.getLimitEntrySize());
		}

		mUpdateCounter++;
		mRoot.put(aEntry);
	}


	public void remove(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		mUpdateCounter++;
		mRoot.remove(aEntry);
	}


	public void visit(BTreeVisitor aVisitor)
	{
		assertNotClosed();

		mRoot.visit(aVisitor, null, null);
	}


	int getBlockSize()
	{
		return mBlockAccessor.getBlockDevice().getBlockSize();
	}


	public void flush()
	{
		balanceTree();
	}


	public boolean commit()
	{
		log.i("commit");
		log.inc();

		assertNotClosed();

		assert integrityCheck() == null : integrityCheck();

		long modCount = mModCount;

		log.i("committing tree");
		log.inc();

		balanceTree();

		if (mRoot.mBlockPointer == null)
		{
			mRoot.mModified = true;
		}
		mRoot.persist();

		log.i("table commit finished; root block is {}", mRoot.mBlockPointer);
		log.dec();

		if (mModCount != modCount)
		{
			throw new IllegalStateException("concurrent modification");
		}

		mConfiguration.putRoot(mRoot.mBlockPointer);

		log.dec();

		return true;
	}


	public void rollback()
	{
		assertNotClosed();
		mUpdateCounter++;
		log.i("rollback");
		initialize();
	}


	/**
	 * Clean-up resources
	 */
	@Override
	public void close()
	{
		mRoot = null;
		mBlockAccessor = null;
	}


	public long size()
	{
		assertNotClosed();

		AtomicLong result = new AtomicLong();

		visit(new BTreeVisitor()
		{
			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
				result.addAndGet(aNode.mMap.size());
				return true;
			}
		});

		return result.get();
	}


	byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.BTREE_NODE && aBlockPointer.getBlockType() != BlockType.BTREE_LEAF && aBlockPointer.getBlockType() != BlockType.HOLE)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	BlockPointer writeBlock(byte[] aContent, int aLevel, int aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, aBlockType, aLevel, aLevel == 0 ? mConfiguration.getLeafCompressor() : mConfiguration.getNodeCompressor());
	}


	void freeBlock(BlockPointer aBlockPointer)
	{
		mBlockAccessor.freeBlock(aBlockPointer);
	}


	private void assertNotClosed()
	{
		if (mRoot == null)
		{
			throw new IllegalStateException("BTree is closed");
		}
	}


	public String integrityCheck()
	{
		log.i("integrity check");

		AtomicReference<String> result = new AtomicReference<>();

		mRoot.visit(new BTreeVisitor()
		{
			@Override
			public boolean beforeAnyNode(BTreeNode aNode)
			{
				String tmp = aNode.integrityCheck();
				if (tmp != null)
				{
					result.set(tmp);
					return false;
				}
				return true;
			}
		}, null, null);

		return result.get();
	}


	BTreeNode getRoot()
	{
		return mRoot;
	}


	public void drop(Consumer<? super ArrayMapEntry> aConsumer)
	{
		visit(new BTreeVisitor()
		{
			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
				aNode.forEachEntry(aConsumer);
				freeBlock(aNode.mBlockPointer);
				return true;
			}


			@Override
			public boolean afterInteriorNode(BTreeInteriorNode aNode)
			{
				freeBlock(aNode.mBlockPointer);
				return true;
			}
		});
	}


//	public void find(ArrayList<Document> aList, Document aFilter, Function<ArrayMapEntry, Document> aDocumentSupplier)
//	{
//		visit(new BTreeVisitor()
//		{
//			@Override
//			public boolean beforeInteriorNode(BTreeInteriorNode aNode, ArrayMapEntry aLowestKey, ArrayMapEntry aHighestKey)
//			{
//				return matchKey(aLowestKey, aHighestKey, aFilter);
//			}
//
//
//			@Override
//			public boolean beforeLeafNode(BTreeLeafNode aNode)
//			{
//				boolean b = matchKey(aNode.mMap.getFirst().getKey(), aNode.mMap.getLast().getKey(), aFilter);
//
////					System.out.println("#" + aNode.mMap.getFirst().getKey()+", "+aNode.mMap.getLast().getKey() +" " + b+ " "+aQuery.getArray("_id"));
//				return b;
//			}
//
//
//			@Override
//			public boolean leaf(BTreeLeafNode aNode)
//			{
//				for (int i = 0; i < aNode.mMap.size(); i++)
//				{
//					ArrayMapEntry entry = aNode.mMap.get(i);
//					Document doc = aDocumentSupplier.apply(entry);
//
//					if (matchKey(doc, aFilter))
//					{
//						aList.add(doc);
//					}
//				}
//				return true;
//			}
//		});
//	}
//	private boolean matchKey(ArrayMapEntry aLowestKey, ArrayMapEntry aHighestKey, Document aQuery)
//	{
//		Array lowestKey = aLowestKey == null ? null : (Array)aLowestKey.get();
//		Array highestKey = aHighestKey == null ? null : (Array)aHighestKey.get();
//		Array array = aQuery.getArray("_id");
//
//		int a = lowestKey == null ? 0 : compare(lowestKey, array);
//		int b = highestKey == null ? 0 : compare(highestKey, array);
//
//		return a >= 0 && b <= 0;
//	}
	@SuppressWarnings("unchecked")
	private int compare(Array aCompare, Array aWith)
	{
		for (int i = 0; i < aWith.size(); i++)
		{
			Comparable v = aWith.get(i);
			Comparable b = aCompare.get(i);

			int r = v.compareTo(b);

			if (r != 0)
			{
				return r;
			}
		}

		return 0;
	}


	@SuppressWarnings("unchecked")
	private boolean matchKey(Document aEntry, Document aQuery)
	{
		Array array = aQuery.getArray("_id");

		for (int i = 0; i < array.size(); i++)
		{
			Comparable v = array.get(i);
			Object b = aEntry.getArray("_id").get(i);

			if (v.compareTo(b) != 0)
			{
				return false;
			}
		}

		return true;
	}


	long getUpdateCounter()
	{
		return mUpdateCounter;
	}


	BTreeLeafNode findLeaf(ArrayMapEntry aEntry)
	{
		BTreeNode node = mRoot;

		while (node instanceof BTreeInteriorNode v)
		{
			if (aEntry == null)
			{
				node = v.getNode(0);
			}
			else
			{
				node = v.getNearestNode(aEntry);
			}
		}

		return (BTreeLeafNode)node;
	}


//	BTreeLeafNode findNextLeaf(ArrayMapKey aKey)
//	{
//		BTreeNode node = mRoot;
//
//		while (node instanceof BTreeInteriorNode v)
//		{
//			if (aKey == null)
//			{
//				node = v.getNode(0);
//			}
//			else
//			{
//				node = v.getNode(v.mArrayMap.size() - 1);
//
//				for (int i = 0; i < v.mArrayMap.size(); i++)
//				{
//					ArrayMapKey a = v.mArrayMap.getKey(i);
//
//					if (aKey.compareTo(a) <= 0)
//					{
//						node = v.getNode(i);
//						break;
//					}
//				}
//			}
//		}
//
//		return (BTreeLeafNode)node;
//	}
	public void printTree()
	{
		if (mRoot instanceof BTreeInteriorNode v)
		{
			printTree(v, "", true);
		}
		if (mRoot instanceof BTreeLeafNode v)
		{
			printTree(v, "");
		}
	}


	private void printTree(BTreeInteriorNode aNode, String aIndent, boolean aLast)
	{
		Array keys = new Array();
		for (int i = 0; i < aNode.mMap.size(); i++)
		{
			keys.add(aNode.mMap.getKey(i).toKeyString().split("-")[0]);
		}
		Console.println(aIndent + (aLast ? "o---" : "+---") + "node ", Document.of("alloc:$,fill%:$,level:$,gen:$,keys:$", aNode.mMap.getCapacity(), aNode.mMap.getUsedSpace() * 100.0 / aNode.mMap.getCapacity(), aNode.mLevel, aNode.mBlockPointer.getGeneration(), keys));
		aIndent += (aLast ? "    " : "|   ");
		for (int i = 0; i < aNode.size(); i++)
		{
			BTreeNode child = aNode.getNode(i);
			if (child instanceof BTreeInteriorNode v)
			{
				printTree(v, aIndent, i == aNode.size() - 1);
			}
			if (child instanceof BTreeLeafNode v)
			{
				printTree(v, aIndent + (i == aNode.size() - 1 ? "o---" : "+---"));
			}
		}
	}


	private void printTree(BTreeLeafNode aNode, String aIndent)
	{
		Array keys = new Array();
		for (int i = 0; i < aNode.mMap.size(); i++)
		{
//			keys.add(aNode.mMap.getKey(i).toKeyString().split("-")[0]);
			keys.add(aNode.mMap.getKey(i).toKeyString());
		}

		Console.println(aIndent + "leaf ", Document.of("alloc:$,fill%:$,level:$,gen:$,keys:$", aNode.mMap.getCapacity(), aNode.mMap.getUsedSpace() * 100.0 / aNode.mMap.getCapacity(), aNode.mLevel, aNode.mBlockPointer == null ? -1 : aNode.mBlockPointer.getGeneration(), keys));
	}


	void schedule(BTreeNode aNode)
	{
		HashSet<BTreeNode> set = mSchedule.get(aNode.mLevel);
		synchronized (set)
		{
			set.add(aNode);
		}
	}


	private void upgrade(BTreeLeafNode aNode)
	{
		BTreeInteriorNode root = new BTreeInteriorNode(this, aNode.mParent, aNode.mLevel + 1, new ArrayMap(mConfiguration.getNodeSize(), getBlockSize()));
		root.mModified = true;

		ArrayMap[] maps = aNode.mMap.splitManyTail(mConfiguration.getLeafSize());

		for (int i = 1; i < maps.length; i++)
		{
			BTreeLeafNode node = new BTreeLeafNode(this, root, maps[i]);
			node.mModified = true;
			ArrayMapEntry key = maps[i].getKey(0);
			ArrayMapEntry entry = new ArrayMapEntry().setKey(key.getKey(), key.getKeyType()).setValue(BLOCKPOINTER_PLACEHOLDER, Type.BLOCKPOINTER);
			root.mMap.insert(entry);
			root.mChildren.put(entry, node);
		}

		aNode.mMap = maps[0];
		aNode.mParent = root;
		aNode.mModified = true;
		ArrayMapEntry entry = new ArrayMapEntry().setKey(new byte[0], Type.FIRST).setValue(BLOCKPOINTER_PLACEHOLDER, Type.BLOCKPOINTER);
		root.mMap.insert(entry);
		root.mChildren.put(entry, aNode);

		mRoot = root;

		assert root.mMap.getKey(0).getKeyType() == Type.FIRST;

		if (root.mMap.getCapacity() > mConfiguration.getNodeSize())
		{
			schedule(root);
		}
	}


	protected void grow(BTreeInteriorNode aNode)
	{
		log.i("growing tree, level: {}", aNode.mLevel + 1);

		assert aNode == mRoot;

		BTreeInteriorNode newRoot = new BTreeInteriorNode(this, null, aNode.mLevel + 1, new ArrayMap(mConfiguration.getNodeSize(), getBlockSize()));
		ArrayMapEntry entry = new ArrayMapEntry().setKey(new byte[0], Type.FIRST).setValue(BLOCKPOINTER_PLACEHOLDER, Type.BLOCKPOINTER);
		newRoot.mMap.insert(entry);
		newRoot.mChildren.put(entry, aNode);
		newRoot.mModified = true;

		aNode.mParent = newRoot;

		mRoot = newRoot;
	}


	private void splitLeaf(BTreeLeafNode aNode)
	{
		if (aNode.mParent == null)
		{
			upgrade(aNode);
			return;
		}

//		ArrayMap[] maps = aNode.mMap.splitManyTail(mConfiguration.getLeafSize());
		ArrayMap[] maps = aNode.mMap.splitMany(mConfiguration.getLeafSize());

		aNode.mMap = maps[0];

		for (int i = 1; i < maps.length; i++)
		{
			BTreeLeafNode node = new BTreeLeafNode(this, aNode.mParent, maps[i]);
			node.mModified = true;
			ArrayMapEntry key = maps[i].getKey(0);
			ArrayMapEntry entry = new ArrayMapEntry().setKey(key.getKey(), key.getKeyType()).setValue(BLOCKPOINTER_PLACEHOLDER, Type.BLOCKPOINTER);
			aNode.mParent.mMap.insert(entry);
			aNode.mParent.mChildren.put(entry, node);
		}

		if (aNode.mParent.mMap.getCapacity() > mConfiguration.getNodeSize())
		{
			schedule(aNode.mParent);
		}
	}


	private void splitNode(BTreeInteriorNode aNode)
	{
		if (aNode.mParent == null)
		{
			grow(aNode);
		}

		ArrayMap[] maps = aNode.mMap.splitMany(mConfiguration.getNodeSize());

		for (int i = maps.length; --i >= 0;)
		{
			ArrayMap map = maps[i];

			if (i == 0)
			{
				aNode.mMap = map;
				aNode.mModified = true;
				aNode.mParent.mModified = true;
			}
			else
			{
				BTreeInteriorNode node = new BTreeInteriorNode(this, aNode.mParent, aNode.mLevel, map);
				node.mModified = true;

				TreeMap<ArrayMapEntry, BTreeNode> nodeChildren = node.mChildren;

				for (int j = 0; j < map.size(); j++)
				{
					ArrayMapEntry childKey = map.getKey(j);
					BTreeNode childNode = aNode.mChildren.remove(childKey);
					if (childNode != null)
					{
						nodeChildren.put(childKey, childNode);
					}
				}

				ArrayMapEntry firstEntry = map.get(0);

				ArrayMapEntry entry = new ArrayMapEntry().setKey(firstEntry.getKey(), firstEntry.getKeyType()).setValue(BLOCKPOINTER_PLACEHOLDER, Type.BLOCKPOINTER);
				aNode.mParent.mMap.insert(entry);
				aNode.mParent.mChildren.put(entry, node);
				aNode.mParent.mModified = true;

				BTreeNode first = nodeChildren.remove(firstEntry);
				if (first != null)
				{
					nodeChildren.put(new ArrayMapEntry().setKey(new byte[0], Type.FIRST), first);
				}

				firstEntry.setKey(new byte[0], Type.FIRST);
				map.remove(0);
				map.put(firstEntry);
			}
		}

		if (aNode.mParent.mMap.getCapacity() > mConfiguration.getNodeSize())
		{
			schedule(aNode.mParent);
		}
	}


//	private int mergeLeaf()
//	{
//			if (a)
//			{
//				a &= aLeftChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aLeftChild).getUsedSpace() <= aSizeLimit;
//			}
//			if (b)
//			{
//				b &= aRghtChild.size() <= aKeyLimit || aCurntChld.size() <= aKeyLimit || ((BTreeInteriorNode)aCurntChld).getUsedSpace() + ((BTreeInteriorNode)aRghtChild).getUsedSpace() <= aSizeLimit;
//			}
//
//			if (a && b)
//			{
//				if (((BTreeInteriorNode)aLeftChild).getFreeSpace() < ((BTreeInteriorNode)aRghtChild).getFreeSpace())
//				{
//					a = false;
//				}
//				else
//				{
//					b = false;
//				}
//			}
//	}
	private void mergeLeaf(BTreeLeafNode aNode)
	{
		if (aNode.mParent == null)
		{
			return;
		}

		System.out.println(aNode.mParent.mChildren.containsValue(aNode));

		int index = aNode.mParent.indexOf(aNode);
		System.out.println(index);

		BTreeNode left = aNode.mParent.getNode(index - 1);
		System.out.println(left);

		BTreeNode right = aNode.mParent.getNode(index + 1);
		System.out.println(right);
	}


	private synchronized void balanceTree()
	{
		log.i("flush");
		log.inc();

		ArrayList<Integer> splits = new ArrayList<>();
		long t = System.currentTimeMillis();

		for (int i = 0; i < mSchedule.size(); i++)
		{
			BTreeNode[] arr;
			HashSet<BTreeNode> set = mSchedule.get(i);
			synchronized (set)
			{
				arr = set.toArray(BTreeNode[]::new);
				set.clear();
			}

			splits.add(arr.length);

			if (arr.length > 0)
			{
//				Console.indent(0).println("flush level ", i);

				for (BTreeNode node : arr)
				{
					if (node instanceof BTreeLeafNode v)
					{
						if (v.mMap.getUsedSpace() < mConfiguration.getLeafSize() / 4)
						{
							mergeLeaf(v);
						}
						else if (v.mMap.getUsedSpace() > mConfiguration.getLeafSize())
						{
							splitLeaf(v);
						}
					}
					else if (node instanceof BTreeInteriorNode v)
					{
						if (v.mMap.getUsedSpace() < mConfiguration.getNodeSize() / 4)
						{
						}
						else if (v.mMap.getUsedSpace() > mConfiguration.getNodeSize())
						{
							splitNode(v);
						}
					}
				}
			}
		}

		log.i(splits.toString());

		log.dec();
	}
}
