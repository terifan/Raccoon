package org.terifan.raccoon.btree;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.terifan.logging.Logger;
import static org.terifan.raccoon.RaccoonCollection.TYPE_TREENODE;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.BlockType;
import org.terifan.raccoon.util.Console;


public class BTree implements AutoCloseable
{
	private final static Logger log = Logger.getLogger();

	static BlockPointer BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL);

	public static boolean RECORD_USE;

	private final BTreeConfiguration mConfiguration;
	private BlockAccessor mBlockAccessor;
	private BTreeNode mRoot;
	private long mModCount;
	private long mUpdateCounter;
//	private Runnable mFlusher;


	public BTree(BlockAccessor aBlockAccessor, BTreeConfiguration aConfiguration)
	{
		assert aBlockAccessor != null;

		mBlockAccessor = aBlockAccessor;
		mConfiguration = aConfiguration;
//		mFlusher = () -> flush();

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


	public OpResult get(ArrayMapKey aKey)
	{
		assertNotClosed();

		return mRoot.get(aKey);
	}

//	private long mDirtyLeafs;
//	private long mSplitLeafs;
//	private boolean mPendingFlush;

	public OpResult put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.length() > mConfiguration.getSizeThreshold())
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().size() + ", value: " + aEntry.length() + ", maximum: " + mConfiguration.getSizeThreshold());
		}

		mUpdateCounter++;
		OpResult result = mRoot.put(aEntry.getKey(), aEntry);

//		if (result.change == NodeState.DIRTY)
//		{
//			mDirtyLeafs++;
//		}
//		if (result.change == NodeState.SPLIT)
//		{
//			mSplitLeafs++;
//		}
//		check();
//		if (!mPendingFlush && mSplitLeafs > 0)
//		{
//			System.out.println("schedule flush");
//			mPendingFlush = true;
//			mFlusher.run();
//		}
		return result;
	}


	public OpResult remove(ArrayMapKey aKey)
	{
		assertNotClosed();

		mUpdateCounter++;
		OpResult result = mRoot.remove(aKey);
		return result;
	}


	public void visit(BTreeVisitor aVisitor)
	{
		assertNotClosed();

		mRoot.visit(aVisitor, null, null);
	}


//	public boolean isChanged()
//	{
//		return mRoot.mModified;
//	}
	int getBlockSize()
	{
		return mBlockAccessor.getBlockDevice().getBlockSize();
	}


//	public void setFlusher(Runnable aFlusher)
//	{
//		mFlusher = aFlusher;
//	}
	public void flush()
	{
		flushImpl();
	}


	public boolean commit()
	{
		log.i("commit");
		log.inc();

		assertNotClosed();

//		if (!mRoot.mModified)
//		{
//			return false;
//		}
		assert integrityCheck() == null : integrityCheck();

		long modCount = mModCount;

		log.i("committing tree");
		log.inc();

		flushImpl();

		mRoot.commit();
		mRoot.postCommit();

		log.d("table commit finished; root block is {}", mRoot.mBlockPointer);
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
		if (aBlockPointer.getBlockType() != BlockType.BTREE_NODE && aBlockPointer.getBlockType() != BlockType.BTREE_LEAF)
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


	public void find(ArrayList<Document> aList, Document aFilter, Function<ArrayMapEntry, Document> aDocumentSupplier)
	{
		visit(new BTreeVisitor()
		{
			@Override
			public boolean beforeInteriorNode(BTreeInteriorNode aNode, ArrayMapKey aLowestKey, ArrayMapKey aHighestKey)
			{
				return matchKey(aLowestKey, aHighestKey, aFilter);
			}


			@Override
			public boolean beforeLeafNode(BTreeLeafNode aNode)
			{
				boolean b = matchKey(aNode.mMap.getFirst().entry.getKey(), aNode.mMap.getLast().entry.getKey(), aFilter);

//					System.out.println("#" + aNode.mMap.getFirst().getKey()+", "+aNode.mMap.getLast().getKey() +" " + b+ " "+aQuery.getArray("_id"));
				return b;
			}


			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
//					System.out.println(aNode);
				for (int i = 0; i < aNode.mMap.size(); i++)
				{
					OpResult op = aNode.mMap.get(i);
					Document doc = aDocumentSupplier.apply(op.entry);

					if (matchKey(doc, aFilter))
					{
						aList.add(doc);
					}
				}
				return true;
			}
		});
	}


	private boolean matchKey(ArrayMapKey aLowestKey, ArrayMapKey aHighestKey, Document aQuery)
	{
		Array lowestKey = aLowestKey == null ? null : (Array)aLowestKey.get();
		Array highestKey = aHighestKey == null ? null : (Array)aHighestKey.get();
		Array array = aQuery.getArray("_id");

		int a = lowestKey == null ? 0 : compare(lowestKey, array);
		int b = highestKey == null ? 0 : compare(highestKey, array);

		return a >= 0 && b <= 0;
	}


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


	BTreeLeafNode findLeaf(ArrayMapKey aKey)
	{
		ArrayMapEntry entry = new ArrayMapEntry(aKey);
		BTreeNode node = mRoot;

		while (node instanceof BTreeInteriorNode v)
		{
			if (aKey == null)
			{
				node = v.getNode(0);
			}
			else
			{
				node = v.__getNearestNode(entry);
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
	public void check()
	{
		if (mRoot instanceof BTreeInteriorNode v)
		{
			check(v, "", true);
		}
		if (mRoot instanceof BTreeLeafNode v)
		{
			check(v, "");
		}
	}


	public void check(BTreeInteriorNode aNode, String aIndent, boolean aLast)
	{
		Array keys = new Array();
		for (int i = 0; i < aNode.mMap.size(); i++)
		{
			keys.add(aNode.mMap.getKey(i).get().toString().split("-")[0]);
		}
		Console.println(aIndent + (aLast ? "o---" : "+---") + "node ", Document.of("alloc:$,fill%:$,keys:$", aNode.mMap.getCapacity(), aNode.mMap.getUsedSpace() * 100.0 / aNode.mMap.getCapacity(), keys));
		aIndent += (aLast ? "    " : "|   ");
		for (int i = 0; i < aNode.size(); i++)
		{
			BTreeNode child = aNode.getNode(i);
			if (child instanceof BTreeInteriorNode v)
			{
				check(v, aIndent, i == aNode.size() - 1);
			}
			if (child instanceof BTreeLeafNode v)
			{
				check(v, aIndent + (i == aNode.size() - 1 ? "o---" : "+---"));
			}
		}
	}


	public void check(BTreeLeafNode aNode, String aIndent)
	{
		Array keys = new Array();
		for (int i = 0; i < aNode.mMap.size(); i++)
		{
			keys.add(aNode.mMap.getKey(i).get().toString().split("-")[0]);
		}

		Console.println(aIndent + "leaf ", Document.of("alloc:$,fill%:$,keys:$", aNode.mMap.getCapacity(), aNode.mMap.getUsedSpace() * 100.0 / aNode.mMap.getCapacity(), keys));
	}

	ArrayList<HashSet<BTreeNode>> mSchedule = new ArrayList<>()
	{
		{
			for (int i = 0; i < 100; i++)
			{
				add(new HashSet<>());
			}
		}
	};


	void schedule(BTreeNode aNode)
	{
		if (mSchedule.get(aNode.mLevel).add(aNode))
		{
			log.i("scheduled " + aNode.getClass().getSimpleName() + " level {}", aNode.mLevel);
		}
	}


	private void upgrade(BTreeLeafNode aNode)
	{
		BTreeInteriorNode root = new BTreeInteriorNode(this, aNode.mParent, aNode.mLevel + 1, new ArrayMap(mConfiguration.getNodeSize(), getBlockSize()));

		ArrayMap[] maps = aNode.mMap.splitManyTail(mConfiguration.getLeafSize());

		for (int i = 0; i < maps.length; i++)
		{
			BTreeLeafNode node = new BTreeLeafNode(this, root, maps[i]);
			ArrayMapKey key = i == 0 ? ArrayMapKey.EMPTY : node.mMap.getKey(0);
			root.mMap.insert(new ArrayMapEntry(key, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			root.mChildren.put(key, node);
		}

		mRoot = root;
		aNode.mParent = root;

		if (root.mMap.getCapacity() > mConfiguration.getNodeSize())
		{
			schedule(root);
		}
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
			ArrayMapKey key = node.mMap.getKey(0);
			aNode.mParent.mMap.insert(new ArrayMapEntry(key, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
			aNode.mParent.mChildren.put(key, node);
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

		for (int i =  maps.length; --i >= 0;)
		{
			TreeMap<ArrayMapKey, BTreeNode> nodeChildren;
			ArrayMap map = maps[i];

			if (i == 0)
			{
				aNode.mMap = map;
			}
			else
			{
				BTreeInteriorNode node = new BTreeInteriorNode(this, aNode.mParent, aNode.mLevel, map);
				nodeChildren = node.mChildren;

				for (int j = 0; j < map.size(); j++)
				{
					ArrayMapKey childKey = map.getKey(j);
					BTreeNode childNode = aNode.mChildren.remove(childKey);
					if (childNode != null)
					{
						nodeChildren.put(childKey, childNode);
					}
				}

				ArrayMapEntry entry = map.get(0).entry;
				ArrayMapKey firstKey = entry.getKey();

				aNode.mParent.mMap.insert(new ArrayMapEntry(firstKey, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
				aNode.mParent.mChildren.put(firstKey, node);

				BTreeNode first = nodeChildren.remove(firstKey);
				if (first != null)
				{
					nodeChildren.put(ArrayMapKey.EMPTY, first);
				}

				map.remove(0);
				entry.setKey(ArrayMapKey.EMPTY);
				map.put(entry);
			}

			System.out.println("---> " + map.keys(e -> "\"" + (e.toString().contains("-") ? e.toString().substring(0, e.toString().indexOf("-")) : "") + "\""));
			System.out.println("xxx> " + aNode.mParent.mMap.keys(e -> "\"" + (e.toString().contains("-") ? e.toString().substring(0, e.toString().indexOf("-")) : "") + "\""));
		}


		if (aNode.mParent.mMap.getCapacity() > mConfiguration.getNodeSize())
		{
			schedule(aNode.mParent);
		}
	}


	protected void grow(BTreeInteriorNode aNode)
	{
		log.i("growing tree, level: {}", aNode.mLevel + 1);

		assert aNode == mRoot;

		BTreeInteriorNode newRoot = new BTreeInteriorNode(this, null, aNode.mLevel + 1, new ArrayMap(mConfiguration.getNodeSize(), getBlockSize()));
		newRoot.mMap.insert(new ArrayMapEntry(ArrayMapKey.EMPTY, BLOCKPOINTER_PLACEHOLDER, TYPE_TREENODE));
		newRoot.mChildren.put(ArrayMapKey.EMPTY, aNode);

		aNode.mParent = newRoot;
		mRoot = newRoot;
	}


	private synchronized void flushImpl()
	{
		log.i("flush");
		log.inc();

		for (int i = 0; i < mSchedule.size(); i++)
		{
			HashSet<BTreeNode> nodes = mSchedule.get(i);

			if (!nodes.isEmpty())
			{
				Console.indent(0).println("flush level ", i);

				for (BTreeNode node : nodes.toArray(BTreeNode[]::new))
				{
					if (node.mLevel == 0)
					{
						Console.indent(mRoot.mLevel - node.mLevel).println("split leaf");

						splitLeaf((BTreeLeafNode)node);
					}
					else
					{
						Console.indent(mRoot.mLevel - node.mLevel).println("split node");

						splitNode((BTreeInteriorNode)node);
					}
				}

				nodes.clear();
			}
		}

		log.dec();
	}
}
