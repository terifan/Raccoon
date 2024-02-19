package org.terifan.raccoon.btree;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.btree.BTreeNode.RemoveResult;
import org.terifan.raccoon.ScanResult;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.BlockType;


public class BTree implements AutoCloseable
{
	private final static Logger log = Logger.getLogger();

	private static final int ENTRY_SIZE_LIMIT = 0;
	private static final int NODE_SIZE = 1;
	private static final int LEAF_SIZE = 2;
	private static final int NODE_COMPRESSOR = 3;
	private static final int LEAF_COMPRESSOR = 4;
	private static final String CONF = "conf";
	private static final String ROOT = "ptr";

	static BlockPointer BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL);

	public static boolean RECORD_USE;

	private BlockAccessor mBlockAccessor;
	private int mCompressorInteriorBlocks;
	private int mCompressorLeafBlocks;
	private Document mConfiguration;
	private BTreeNode mRoot;
	private long mModCount;
	private int mEntrySizeLimit;
	private int mLeafSize;
	private int mNodeSize;
	private long mUpdateCounter;


	public BTree(BlockAccessor aBlockAccessor, Document aConfiguration)
	{
		assert aBlockAccessor != null;
		assert aConfiguration != null;

		mBlockAccessor = aBlockAccessor;
		mConfiguration = aConfiguration;

		Array conf = mConfiguration.getArray(CONF);
		mCompressorInteriorBlocks = conf.getInt(NODE_COMPRESSOR);
		mCompressorLeafBlocks = conf.getInt(LEAF_COMPRESSOR);
		mEntrySizeLimit = conf.getInt(ENTRY_SIZE_LIMIT);
		mLeafSize = conf.getInt(LEAF_SIZE);
		mNodeSize = conf.getInt(NODE_SIZE);

		initialize();
	}


	private void initialize()
	{
		if (mConfiguration.containsKey(ROOT))
		{
			log.i("open table");
			log.inc();
			BlockPointer bp = new BlockPointer().unmarshal(mConfiguration.get(ROOT));
			mRoot = bp.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(this, null, bp.getBlockLevel(), new ArrayMap(readBlock(bp))) : new BTreeLeafNode(this, null, new ArrayMap(readBlock(bp)));
			mRoot.mBlockPointer = bp;
			log.dec();
		}
		else
		{
			log.i("create table");
			log.inc();
			mRoot = new BTreeLeafNode(this, null, new ArrayMap(mLeafSize));
			log.dec();
		}
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		return mRoot.get(aEntry.getKey(), aEntry);
	}


	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.length() > mEntrySizeLimit)
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().size() + ", value: " + aEntry.length() + ", maximum: " + mEntrySizeLimit);
		}

		mUpdateCounter++;

		log.i("put");
		log.inc();

		Result<ArrayMapEntry> result = new Result<>();

		if (mRoot instanceof BTreeLeafNode v)
		{
			if (v.mMap.getCapacity() > mLeafSize || v.mMap.getFreeSpace() < aEntry.getMarshalledLength())
			{
				_upgrade();
			}
		}
		else if (mRoot instanceof BTreeInteriorNode v)
		{
			if (v.getUsedSpace() > mNodeSize)
			{
				_grow();
			}
		}

		mRoot.put(aEntry.getKey(), aEntry, result);

		log.dec();

		return result.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		mUpdateCounter++;

		log.i("put");
		log.inc();

		Result<ArrayMapEntry> prev = new Result<>();

		RemoveResult result = mRoot.remove(aEntry.getKey(), prev);

		if (result == RemoveResult.REMOVED)
		{
			if (mRoot.mLevel > 1 && mRoot.size() == 1)
			{
				_shrink();
			}
			if (mRoot.mLevel == 1 && mRoot.size() == 1)
			{
				_downgrade();
			}
		}

		log.dec();

		return prev.get();
	}


	public void visit(BTreeVisitor aVisitor)
	{
		assertNotClosed();

		log.i("visit");
		log.inc();

		mRoot.visit(aVisitor, null, null);

		log.dec();
	}


	public boolean isChanged()
	{
		return mRoot.mModified;
	}


	public long flush()
	{
		return 0L;
	}


	public boolean commit()
	{
		assertNotClosed();

		if (!mRoot.mModified)
		{
			return false;
		}

		long modCount = mModCount;

		log.i("committing table");
		log.inc();

		assert integrityCheck() == null : integrityCheck();

		mRoot.commit();
		mRoot.postCommit();

		log.d("table commit finished; root block is {}", mRoot.mBlockPointer);
		log.dec();

		if (mModCount != modCount)
		{
			throw new IllegalStateException("concurrent modification");
		}

		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeInteriorNode ? BlockType.BTREE_NODE : BlockType.BTREE_LEAF);

		mConfiguration.put(ROOT, mRoot.mBlockPointer.marshal());

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


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.BTREE_NODE && aBlockPointer.getBlockType() != BlockType.BTREE_LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(byte[] aContent, int aLevel, int aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, aBlockType, aLevel, aLevel == 0 ? mCompressorLeafBlocks : mCompressorInteriorBlocks);
	}


	protected void freeBlock(BlockPointer aBlockPointer)
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


	public Document getConfiguration()
	{
		return mConfiguration;
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


	public ScanResult scan(ScanResult aScanResult)
	{
		aScanResult.tables++;

		mRoot.scan(aScanResult);

		return aScanResult;
	}


	@Deprecated
	public static Document createDefaultConfig(int aBlockSize)
	{
		Array conf = new Array();
		conf.put(BTree.ENTRY_SIZE_LIMIT, 1024);
		conf.put(BTree.NODE_SIZE, Math.max(4096, aBlockSize));
		conf.put(BTree.LEAF_SIZE, Math.max(4096, aBlockSize));
		conf.put(BTree.NODE_COMPRESSOR, CompressorAlgorithm.ZLE.ordinal());
		conf.put(BTree.LEAF_COMPRESSOR, CompressorAlgorithm.LZJB.ordinal());
		return new Document().put(CONF, conf);
	}


	int getLeafSize()
	{
		return mLeafSize;
	}


	int getNodeSize()
	{
		return mNodeSize;
	}


	public int getEntrySizeLimit()
	{
		return mEntrySizeLimit;
	}


	BTreeNode _root()
	{
		return mRoot;
	}


	protected void _grow()
	{
		mRoot = ((BTreeInteriorNode)mRoot).grow();
	}


	protected void _upgrade()
	{
		mRoot = ((BTreeLeafNode)mRoot).upgrade();
	}


	protected void _downgrade()
	{
		mRoot = ((BTreeInteriorNode)mRoot).downgrade();
	}


	protected void _shrink()
	{
		mRoot = ((BTreeInteriorNode)mRoot).shrink();
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
				boolean b = matchKey(aNode.mMap.getFirst().getKey(), aNode.mMap.getLast().getKey(), aFilter);

//					System.out.println("#" + aNode.mMap.getFirst().getKey()+", "+aNode.mMap.getLast().getKey() +" " + b+ " "+aQuery.getArray("_id"));
				return b;
			}


			@Override
			public boolean leaf(BTreeLeafNode aNode)
			{
//					System.out.println(aNode);
				for (int i = 0; i < aNode.mMap.size(); i++)
				{
					ArrayMapEntry entry = aNode.mMap.get(i, new ArrayMapEntry());
					Document doc = aDocumentSupplier.apply(entry);

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


	BTreeLeafNode findNextLeaf(ArrayMapKey aKey)
	{
		BTreeNode node = mRoot;

		while (node instanceof BTreeInteriorNode v)
		{
			if (aKey == null)
			{
				node = v.getNode(0);
			}
			else
			{
				node = v.getNode(v.mArrayMap.size() - 1);

				for (int i = 0; i < v.mArrayMap.size(); i++)
				{
					ArrayMapKey a = v.mArrayMap.getKey(i);

					if (aKey.compareTo(a) <= 0)
					{
						node = v.getNode(i);
						break;
					}
				}
			}
		}

		return (BTreeLeafNode)node;
	}
}
