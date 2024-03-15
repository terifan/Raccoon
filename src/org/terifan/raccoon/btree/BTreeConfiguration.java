package org.terifan.raccoon.btree;

import org.terifan.raccoon.blockdevice.BlockPointer;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.document.Document;


public class BTreeConfiguration extends Document
{
	private final static long serialVersionUID = 1L;

	private final static String ROOT = "root";
	private final static String LIMIT_SIZE = "lesz";
	private final static String NODE_SIZE = "nosz";
	private final static String LEAF_SIZE = "lfsz";
	private final static String NODE_COMPRESSOR = "nocp";
	private final static String LEAF_COMPRESSOR = "lfcp";

	private final int mLimitEntrySize;
	private final int mNodeSize;
	private final int mLeafSize;
	private final int mNodeCompressor;
	private final int mLeafCompressor;


	public BTreeConfiguration()
	{
		this(new Document());
	}


	public BTreeConfiguration(Document aDocument)
	{
		putAll(aDocument);

		mLimitEntrySize = computeIfAbsent(LIMIT_SIZE, e -> 1024);
		mNodeSize = computeIfAbsent(NODE_SIZE, e -> 4096);
		mLeafSize = computeIfAbsent(LEAF_SIZE, e -> 4096);
		mNodeCompressor = computeIfAbsent(NODE_COMPRESSOR, e -> CompressorAlgorithm.ZLE.ordinal());
		mLeafCompressor = computeIfAbsent(LEAF_COMPRESSOR, e -> CompressorAlgorithm.LZJB.ordinal());
	}


	BlockPointer getRoot()
	{
		return containsKey(ROOT) ? BlockPointer.fromByteArray(getBinary(ROOT)) : null;
	}


	void putRoot(BlockPointer aBlockPointer)
	{
		put(ROOT, aBlockPointer.toByteArray());
	}


	public int getLimitEntrySize()
	{
		return mLimitEntrySize;
	}


	int getNodeCompressor()
	{
		return mNodeCompressor;
	}


	int getLeafCompressor()
	{
		return mLeafCompressor;
	}


	int getLeafSize()
	{
		return mLeafSize;
	}


	int getNodeSize()
	{
		return mNodeSize;
	}


	@Override
	public String toString()
	{
		return "BTreeConfiguration{" + "mSizeThreshold=" + mLimitEntrySize + ", mNodeSize=" + mNodeSize + ", mLeafSize=" + mLeafSize + ", mNodeCompressor=" + mNodeCompressor + ", mLeafCompressor=" + mLeafCompressor + ", root=" + getRoot() + '}';
	}
}
