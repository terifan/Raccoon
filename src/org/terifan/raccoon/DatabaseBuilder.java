package org.terifan.raccoon;

import java.io.IOException;
import java.util.ArrayList;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.EncryptionFunction;
import org.terifan.raccoon.io.secure.KeyGenerationFunction;
import org.terifan.raccoon.io.secure.SecureBlockDevice;


public class DatabaseBuilder
{
	private IPhysicalBlockDevice mBlockDevice;
	private boolean mReadOnly;
	private String mLabel;
	private String mPassword;
	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGenerationFunction;
	private int mLazyWriteCacheSizeBlocks;
	private int mPagesPerNode;
	private int mPagesPerLeaf;
	private int mCompressionOfNodes;
	private int mCompressionOfLeafs;
	private int mCompressionOfBlobs;
	private int mBlockReadCacheSize;


	public DatabaseBuilder(IPhysicalBlockDevice aBlockDevice)
	{
		mBlockDevice = aBlockDevice;

		mPagesPerNode = TableParam.DEFAULT.getPagesPerNode();
		mPagesPerLeaf = TableParam.DEFAULT.getPagesPerLeaf();
		mBlockReadCacheSize = TableParam.DEFAULT.getBlockReadCacheSize();
		mCompressionOfNodes = CompressionParam.NONE;
		mCompressionOfLeafs = CompressionParam.NONE;
		mCompressionOfBlobs = CompressionParam.NONE;
		mLazyWriteCacheSizeBlocks = Constants.DEFAULT_LAZY_WRITE_CACHE_SIZE;
		mEncryptionFunction = EncryptionFunction.AES;
		mKeyGenerationFunction = KeyGenerationFunction.SHA512;
	}


	private Database build(OpenOption aOpenOption) throws IOException
	{
		ArrayList<Object> params = new ArrayList<>();

		params.add(new CompressionParam(mCompressionOfNodes, mCompressionOfLeafs, mCompressionOfBlobs));
		params.add(new TableParam(mPagesPerNode, mPagesPerLeaf, mBlockReadCacheSize));

		if (mPassword != null)
		{
			mBlockDevice = new SecureBlockDevice(mBlockDevice, new AccessCredentials(mPassword, mEncryptionFunction, mKeyGenerationFunction));
		}

		IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(mBlockDevice, mLabel, mLazyWriteCacheSizeBlocks);

		return Database.open(managedBlockDevice, aOpenOption, params.toArray());
	}


	/**
	 * Create a new or open an existing database.
	 */
	public Database create() throws IOException
	{
		return build(OpenOption.CREATE);
	}


	/**
	 * Create a new empty database erasing any existing data.
	 */
	public Database reset() throws IOException
	{
		return build(OpenOption.CREATE_NEW);
	}


	/**
	 * Open an existing database.
	 */
	public Database open() throws IOException
	{
		return build(mReadOnly ? OpenOption.READ_ONLY : OpenOption.OPEN);
	}


	public DatabaseBuilder setReadOnly(boolean aReadOnly)
	{
		mReadOnly = aReadOnly;
		return this;
	}


	public DatabaseBuilder setLazyWriteCacheSizeBlocks(int aLazyWriteCacheSizeBlocks)
	{
		mLazyWriteCacheSizeBlocks = aLazyWriteCacheSizeBlocks;
		return this;
	}


	public DatabaseBuilder setPassword(String aPassword)
	{
		mPassword = aPassword;
		return this;
	}


	public DatabaseBuilder setEncryption(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public DatabaseBuilder setKeyGeneration(KeyGenerationFunction aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction;
		return this;
	}


	public DatabaseBuilder setLabel(String aLabel)
	{
		mLabel = aLabel;
		return this;
	}


	public DatabaseBuilder setPagesPerNode(int aPageCount)
	{
		mPagesPerNode = aPageCount;
		return this;
	}


	public DatabaseBuilder setPagesPerLeaf(int aPageCount)
	{
		mPagesPerLeaf = aPageCount;
		return this;
	}


	public DatabaseBuilder setCompression(CompressionParam aCompressionParam)
	{
		mCompressionOfNodes = aCompressionParam.getNode();
		mCompressionOfLeafs = aCompressionParam.getLeaf();
		mCompressionOfBlobs = aCompressionParam.getBlob();
		return this;
	}


	public DatabaseBuilder setCompressionOfNodes(int aCompressionOfNodes)
	{
		mCompressionOfNodes = aCompressionOfNodes;
		return this;
	}


	public DatabaseBuilder setCompressionOfLeafs(int aCompressionOfLeafs)
	{
		mCompressionOfLeafs = aCompressionOfLeafs;
		return this;
	}


	public DatabaseBuilder setCompressionOfBlobs(int aCompressionOfBlobs)
	{
		mCompressionOfBlobs = aCompressionOfBlobs;
		return this;
	}


	public DatabaseBuilder setBlockReadCacheSize(int aBlockReadCacheSize)
	{
		mBlockReadCacheSize = aBlockReadCacheSize;
		return this;
	}
}
