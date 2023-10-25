package org.terifan.raccoon;

import org.terifan.raccoon.blockdevice.BlockType;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.terifan.raccoon.document.Document;
import org.terifan.raccoon.BTreeNode.RemoveResult;
import org.terifan.raccoon.blockdevice.BlockAccessor;
import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.compressor.CompressorLevel;
import org.terifan.raccoon.blockdevice.util.Log;
import org.terifan.raccoon.document.Array;
import org.terifan.raccoon.util.AbortIteratorException;
import org.terifan.raccoon.util.Result;


public class BTree implements AutoCloseable
{
	static final String ENTRY_SIZE_LIMIT = "0";
	static final String INT_BLOCK_SIZE = "1";
	static final String LEAF_BLOCK_SIZE = "2";
	static final String INT_BLOCK_COMPRESSOR = "3";
	static final String LEAF_BLOCK_COMPRESSOR = "4";
	static final String ROOT = "5";

	static BlockPointer BLOCKPOINTER_PLACEHOLDER = new BlockPointer().setBlockType(BlockType.ILLEGAL);

	public static boolean RECORD_USE;

	private BlockAccessor mBlockAccessor;
	private CompressorLevel mCompressorInteriorBlocks;
	private CompressorLevel mCompressorLeafBlocks;
	private Document mConfiguration;
	private BTreeNode mRoot;
	private long mModCount;


	public BTree(BlockAccessor aBlockAccessor, Document aConfiguration)
	{
		assert aBlockAccessor != null;
		assert aConfiguration != null;

		mBlockAccessor = aBlockAccessor;
		mConfiguration = aConfiguration;

		if (mConfiguration == null)
		{
			mConfiguration = BTree.createDefaultConfig();
		}

		mCompressorInteriorBlocks = CompressorLevel.values()[mConfiguration.getInt(INT_BLOCK_COMPRESSOR)];
		mCompressorLeafBlocks = CompressorLevel.values()[mConfiguration.getInt(LEAF_BLOCK_COMPRESSOR)];

		if (mConfiguration.containsKey(ROOT))
		{
			Log.i("open table %s", mConfiguration.get("name", "?"));
			Log.inc();

			unmarshalHeader();

			Log.dec();
		}
		else
		{
			Log.i("create table %s", mConfiguration.get("name", "?"));
			Log.inc();

			setupEmptyTable();

			Log.dec();
		}
	}


	private void marshalHeader()
	{
		mRoot.mBlockPointer.setBlockType(mRoot instanceof BTreeInteriorNode ? BlockType.TREE_NODE : BlockType.TREE_LEAF);

		mConfiguration.put(ROOT, mRoot.mBlockPointer.marshal());
	}


	private void unmarshalHeader()
	{
		BlockPointer bp = new BlockPointer().unmarshal(mConfiguration.get(ROOT));

		mRoot = bp.getBlockType() == BlockType.TREE_NODE ? new BTreeInteriorNode(bp.getBlockLevel()) : new BTreeLeafNode();
		mRoot.mBlockPointer = bp;
		mRoot.mMap = new ArrayMap(readBlock(bp));
	}


	private void setupEmptyTable()
	{
		mRoot = new BTreeLeafNode();
		mRoot.mMap = new ArrayMap(mConfiguration.getInt(LEAF_BLOCK_SIZE));
	}


	public boolean get(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		return mRoot.get(this, aEntry.getKey(), aEntry);
	}


	public ArrayMapEntry put(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		if (aEntry.length() > mConfiguration.getInt(ENTRY_SIZE_LIMIT))
		{
			throw new IllegalArgumentException("Combined length of key and value exceed maximum length: key: " + aEntry.getKey().size() + ", value: " + aEntry.length() + ", maximum: " + mConfiguration.getInt(ENTRY_SIZE_LIMIT));
		}

		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> result = new Result<>();

		if (mRoot.mLevel == 0 ? mRoot.mMap.getCapacity() > mConfiguration.getInt(LEAF_BLOCK_SIZE) || mRoot.mMap.getFreeSpace() < aEntry.getMarshalledLength() : mRoot.mMap.getUsedSpace() > mConfiguration.getInt(INT_BLOCK_SIZE))
		{
			if (mRoot instanceof BTreeLeafNode)
			{
				mRoot = ((BTreeLeafNode)mRoot).upgrade(this);
			}
			else
			{
				mRoot = ((BTreeInteriorNode)mRoot).grow(this);
			}
		}

		mRoot.put(this, aEntry.getKey(), aEntry, result);

		Log.dec();

		return result.get();
	}


	public ArrayMapEntry remove(ArrayMapEntry aEntry)
	{
		assertNotClosed();

		Log.i("put");
		Log.inc();

		Result<ArrayMapEntry> prev = new Result<>();

		RemoveResult result = mRoot.remove(this, aEntry.getKey(), prev);

		if (result == RemoveResult.REMOVED)
		{
			if (mRoot.mLevel > 1 && ((BTreeInteriorNode)mRoot).mMap.size() == 1)
			{
				mRoot = ((BTreeInteriorNode)mRoot).shrink(this);
			}
			if (mRoot.mLevel == 1 && ((BTreeInteriorNode)mRoot).mMap.size() == 1)
			{
				mRoot = ((BTreeInteriorNode)mRoot).downgrade(this);
			}
		}

		Log.dec();

		return prev.get();
	}


	void visit(BTreeVisitor aVisitor)
	{
		assertNotClosed();

		Log.i("visit");
		Log.inc();

		try
		{
			mRoot.visit(this, aVisitor, null, null);
		}
		catch (AbortIteratorException e)
		{
			// ignore
		}

		Log.dec();
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

		Log.i("commit table");
		Log.inc();

		assert integrityCheck() == null : integrityCheck();

		mRoot.commit(this);
		mRoot.postCommit();

		Log.i("table commit finished; root block is %s", mRoot.mBlockPointer);

		Log.dec();

		if (mModCount != modCount)
		{
			throw new IllegalStateException("concurrent modification");
		}

		marshalHeader();

		return true;
	}


	public void rollback()
	{
		assertNotClosed();

		Log.i("rollback");

		if (mConfiguration.containsKey(ROOT))
		{
			Log.d("rollback");

			unmarshalHeader();
		}
		else
		{
			Log.d("rollback empty");

			setupEmptyTable();
		}
	}


	/**
	 * Clean-up resources
	 */
	@Override
	public void close()
	{
		if (mConfiguration != null)
		{
			mRoot = null;
			mBlockAccessor = null;
			mConfiguration = null;
		}
	}


	public long size()
	{
		assertNotClosed();

		AtomicLong result = new AtomicLong();

		visit(new BTreeVisitor()
		{
			@Override
			boolean leaf(BTree aImplementation, BTreeLeafNode aNode)
			{
				result.addAndGet(aNode.mMap.size());
				return true;
			}
		});

		return result.get();
	}


	protected byte[] readBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer.getBlockType() != BlockType.TREE_NODE && aBlockPointer.getBlockType() != BlockType.TREE_LEAF)
		{
			throw new IllegalArgumentException("Attempt to read bad block: " + aBlockPointer);
		}

		return mBlockAccessor.readBlock(aBlockPointer);
	}


	protected BlockPointer writeBlock(byte[] aContent, int aLevel, int aBlockType)
	{
		return mBlockAccessor.writeBlock(aContent, 0, aContent.length, aBlockType, aLevel, aLevel == 0 ? mCompressorLeafBlocks : mCompressorInteriorBlocks);
	}


	protected void freeBlock(BlockPointer aBlockPointer)
	{
		if (aBlockPointer != null)
		{
			mBlockAccessor.freeBlock(aBlockPointer);
		}
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
		Log.i("integrity check");

		AtomicReference<String> result = new AtomicReference<>();

		mRoot.visit(this, new BTreeVisitor()
		{
			@Override
			boolean beforeAnyNode(BTree aImplementation, BTreeNode aNode)
			{
				String tmp = aNode.mMap.integrityCheck();
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

		scan(mRoot, aScanResult, 0);

		return aScanResult;
	}


	private void scan(BTreeNode aNode, ScanResult aScanResult, int aLevel)
	{
		if (aNode instanceof BTreeInteriorNode)
		{
			BTreeInteriorNode interiorNode = (BTreeInteriorNode)aNode;

			int fillRatio = interiorNode.mMap.getUsedSpace() * 100 / mConfiguration.getInt(INT_BLOCK_SIZE);
			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");

			boolean first = true;
			aScanResult.log.append("'");
			for (ArrayMapEntry entry : interiorNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(":");
				}
				first = false;
				String s = stringifyKey(entry);
				aScanResult.log.append(s.isEmpty() ? "*" : s);
			}
			aScanResult.log.append("'");

			if (aNode.mHighlight)
			{
				aScanResult.log.append("#a00#a00#fff");
			}
			else if (interiorNode.mMap.size() == 1)
			{
				aScanResult.log.append("#000#ff0#000");
			}
			else if (fillRatio > 100)
			{
				aScanResult.log.append(interiorNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(interiorNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}

			first = true;
			aScanResult.log.append("[");

			for (int i = 0, sz = interiorNode.mMap.size(); i < sz; i++)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;

				BTreeNode child = interiorNode.getNode(this, i);

				ArrayMapEntry entry = new ArrayMapEntry();
				interiorNode.mMap.get(i, entry);
				interiorNode.mChildNodes.put(entry.getKey(), child);

				scan(child, aScanResult, aLevel + 1);
			}

			aScanResult.log.append("]");
		}
		else
		{
			int fillRatio = aNode.mMap.getUsedSpace() * 100 / mConfiguration.getInt(LEAF_BLOCK_SIZE);

			aScanResult.log.append("{" + (aNode.mBlockPointer == null ? "" : aNode.mBlockPointer.getBlockIndex0()) + ":" + fillRatio + "%" + "}");
			aScanResult.log.append("[");

			boolean first = true;

			for (ArrayMapEntry entry : aNode.mMap)
			{
				if (!first)
				{
					aScanResult.log.append(",");
				}
				first = false;
				aScanResult.log.append("'" + stringifyKey(entry) + "'");
			}

			aScanResult.log.append("]");

			if (aNode.mHighlight)
			{
				aScanResult.log.append("#a00#a00#fff");
			}
			else if (fillRatio > 100)
			{
				aScanResult.log.append(aNode.mModified ? "#a00#a00#fff" : "#666#666#fff");
			}
			else
			{
				aScanResult.log.append(aNode.mModified ? "#f00#f00#fff" : "#888#fff#000");
			}
		}
	}


	private String stringifyKey(ArrayMapEntry aEntry)
	{
		Object keyValue = aEntry.getKey().get();

		String value = "";

		if (keyValue instanceof Array)
		{
			for (Object k : (Array)keyValue)
			{
				if (!value.isEmpty())
				{
					value += ",";
				}
				value += k.toString().replaceAll("[^\\w]*", "");
			}
		}
		else
		{
			value += keyValue.toString().replaceAll("[^\\w]*", "");
		}

		return value;
	}


	static Document createDefaultConfig()
	{
		return new Document()
			.put(BTree.INT_BLOCK_SIZE, 4096)
			.put(BTree.LEAF_BLOCK_SIZE, 4096)
			.put(BTree.ENTRY_SIZE_LIMIT, 1024)
			.put(BTree.INT_BLOCK_COMPRESSOR, CompressorLevel.DEFLATE_FAST.ordinal())
			.put(BTree.LEAF_BLOCK_COMPRESSOR, CompressorLevel.ZLE.ordinal());
	}
}
