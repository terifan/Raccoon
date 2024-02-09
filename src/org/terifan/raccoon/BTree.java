package org.terifan.raccoon;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.terifan.logging.Logger;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.BTreeNode.RemoveResult;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.util.Result;
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
			mRoot = bp.getBlockType() == BlockType.BTREE_NODE ? new BTreeInteriorNode(this, bp.getBlockLevel(), new ArrayMap(readBlock(bp))) : new BTreeLeafNode(this, new ArrayMap(readBlock(bp)));
			mRoot.mBlockPointer = bp;
			log.dec();
		}
		else
		{
			log.i("create table");
			log.inc();
			mRoot = new BTreeLeafNode(this, new ArrayMap(mLeafSize));
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
			if (v.mChildNodes.getUsedSpace() > mNodeSize)
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


	void visit(BTreeVisitor aVisitor)
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
			boolean leaf(BTreeLeafNode aNode)
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
			boolean beforeAnyNode(BTreeNode aNode)
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
	static Document createDefaultConfig(int aBlockSize)
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


	int getEntrySizeLimit()
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
}
