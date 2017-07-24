package org.terifan.raccoon;

import java.io.IOException;
import java.util.ArrayList;
import org.terifan.raccoon.io.managed.DeviceHeader;
import org.terifan.raccoon.io.managed.IManagedBlockDevice;
import org.terifan.raccoon.io.managed.ManagedBlockDevice;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.raccoon.io.secure.AccessCredentials;
import org.terifan.raccoon.io.secure.CipherModeFunction;
import org.terifan.raccoon.io.secure.EncryptionFunction;
import org.terifan.raccoon.io.secure.KeyGenerationFunction;
import org.terifan.raccoon.io.secure.SecureBlockDevice;


public class DatabaseBuilder
{
	private IPhysicalBlockDevice mBlockDevice;
	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGenerationFunction;
	private CipherModeFunction mCipherModeFunction;
	private int mPagesPerNode;
	private int mPagesPerLeaf;
	private int mCompressionOfNodes;
	private int mCompressionOfLeafs;
	private int mCompressionOfBlobs;
	private int mBlockReadCacheSize;
	private int mIterationCount;
	private boolean mReadOnly;
	private char[] mPassword;
	private DeviceHeader mDeviceHeader;


	public DatabaseBuilder(IPhysicalBlockDevice aBlockDevice)
	{
		mBlockDevice = aBlockDevice;

		mPagesPerNode = TableParam.DEFAULT_PAGES_PER_NODE;
		mPagesPerLeaf = TableParam.DEFAULT_PAGES_PER_LEAF;
		mBlockReadCacheSize = TableParam.DEFAULT_READ_CACHE_SIZE;
		mCompressionOfNodes = CompressionParam.NONE;
		mCompressionOfLeafs = CompressionParam.NONE;
		mCompressionOfBlobs = CompressionParam.NONE;
		mEncryptionFunction = AccessCredentials.DEFAULT_ENCRYPTION;
		mKeyGenerationFunction = AccessCredentials.DEFAULT_KEY_GENERATOR;
		mCipherModeFunction = AccessCredentials.DEFAULT_CIPHER_MODE;
		mIterationCount = AccessCredentials.DEFAULT_ITERATION_COUNT;
		mDeviceHeader = new DeviceHeader("");
	}


	private Database build(OpenOption aOpenOption) throws IOException
	{
		ArrayList<OpenParam> params = new ArrayList<>();

		params.add(new CompressionParam(mCompressionOfNodes, mCompressionOfLeafs, mCompressionOfBlobs));
		params.add(new TableParam(mPagesPerNode, mPagesPerLeaf, mBlockReadCacheSize));

		if (mPassword != null)
		{
			AccessCredentials accessCredentials = new AccessCredentials(mPassword)
				.setEncryptionFunction(mEncryptionFunction)
				.setKeyGeneratorFunction(mKeyGenerationFunction)
				.setCipherModeFunction(mCipherModeFunction)
				.setIterationCount(mIterationCount);

			if (mBlockDevice.length() == 0)
			{
				mBlockDevice = SecureBlockDevice.create(accessCredentials, mBlockDevice);
			}
			else
			{
				mBlockDevice = SecureBlockDevice.open(mBlockDevice, accessCredentials);
			}

			if (mBlockDevice == null)
			{
				throw new InvalidPasswordException("Incorrect password or not a secure block device");
			}
		}

		IManagedBlockDevice managedBlockDevice = new ManagedBlockDevice(mBlockDevice, Constants.DEVICE_HEADER, mDeviceHeader);

		return new Database(managedBlockDevice, aOpenOption, params.toArray(new OpenParam[params.size()]));
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


	public DatabaseBuilder setPassword(String aPassword)
	{
		mPassword = aPassword.toCharArray();
		return this;
	}


	public DatabaseBuilder setPassword(char[] aPassword)
	{
		mPassword = aPassword.clone();
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


	public DatabaseBuilder setCipherModeFunction(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public DatabaseBuilder setLabel(String aLabel)
	{
		mDeviceHeader.setLabel(aLabel);
		return this;
	}


	public DatabaseBuilder setVersion(int aMajorVersion, int aMinorVersion)
	{
		mDeviceHeader.setMajorVersion(aMajorVersion);
		mDeviceHeader.setMinorVersion(aMinorVersion);
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


	/**
	 * Passwords are expanded into cryptographic keys by iterating a hash function this many times. A larger number means more security but
	 * also longer time to open a database.
	 *
	 * WARNING: this value is not recorded in the database file and must be provided when opening a database!
	 *
	 * @param aIterationCount
	 *   the iteration count used.
	 */
	public DatabaseBuilder setIterationCount(int aIterationCount)
	{
		mIterationCount = aIterationCount;
		return this;
	}
}
