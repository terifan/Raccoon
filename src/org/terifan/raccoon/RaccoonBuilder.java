package org.terifan.raccoon;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.document.ObjectId;


public class RaccoonBuilder
{
	protected Path mPath;
	protected EncryptionFunction mEncryptionFunction;
	protected KeyGenerationFunction mKeyGenerationFunction;
	protected CipherModeFunction mCipherModeFunction;
	protected char[] mPassword;
	protected BlockStorage mBlockStorage;
	protected ManagedBlockDevice mManagedBlockDevice;
	protected int mKeyGeneratorIterationCount;
	protected CompressorAlgorithm mCompressorAlgorithm;
//	protected int mDefaultBTreebNodeSize;
//	protected int mDefaultBTreeLeafSize;
//	protected int mDefaultLobNodeSize;
//	protected int mDefaultLobLeafSize;
//	protected SyncMode mSyncMode;


	public RaccoonBuilder()
	{
		mKeyGeneratorIterationCount = 1000;
		mEncryptionFunction = EncryptionFunction.AES;
		mCipherModeFunction = CipherModeFunction.XTS;
		mKeyGenerationFunction = KeyGenerationFunction.SHA512;
		mCompressorAlgorithm = CompressorAlgorithm.LZJB;
	}


	public RaccoonDatabase get()
	{
		return get(DatabaseOpenOption.OPEN);
	}


	public RaccoonDatabase get(DatabaseOpenOption aDatabaseOpenOption)
	{
		if (mPath == null && mManagedBlockDevice == null && mBlockStorage == null)
		{
			throw new IllegalArgumentException("Path not provided.");
		}

		AccessCredentials ac = mPassword == null ? null : new AccessCredentials(mPassword, mEncryptionFunction, mKeyGenerationFunction, mCipherModeFunction).setIterationCount(mKeyGeneratorIterationCount);

		if (mManagedBlockDevice != null)
		{
			return new RaccoonDatabase(mManagedBlockDevice, aDatabaseOpenOption, ac);
		}
		if (mBlockStorage != null)
		{
			return new RaccoonDatabase(mBlockStorage, aDatabaseOpenOption, ac);
		}
		return new RaccoonDatabase(mPath, aDatabaseOpenOption, ac);
	}


	public RaccoonBuilder path(File aPath)
	{
		return RaccoonBuilder.this.path(aPath.toPath());
	}


	public RaccoonBuilder path(String aPath)
	{
		return RaccoonBuilder.this.path(Paths.get(aPath));
	}


	public RaccoonBuilder path(Path aPath)
	{
		mManagedBlockDevice = null;
		mBlockStorage = null;
		mPath = aPath;
		return this;
	}


	public RaccoonBuilder path(BlockStorage aBlockStorage)
	{
		mPath = null;
		mManagedBlockDevice = null;
		mBlockStorage = aBlockStorage;
		return this;
	}


	public RaccoonBuilder path(ManagedBlockDevice aManagedBlockDevice)
	{
		mPath = null;
		mBlockStorage = null;
		mManagedBlockDevice = aManagedBlockDevice;
		return this;
	}


	/**
	 * The database will be stored in a file in the system temporary directory with a random name.
	 */
	public RaccoonBuilder pathInTempDir()
	{
		File dir = new File(System.getProperty("java.io.tmpdir"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("No temporary directory exists in this environment.");
		}
		for (;;)
		{
			File file = new File(dir, ObjectId.randomId() + ".rdb");
			if (!file.exists())
			{
				return RaccoonBuilder.this.path(file);
			}
		}
	}


	/**
	 * The database will be stored in a file in the system temporary directory with the name provided.
	 */
	public RaccoonBuilder pathInTempDir(String aName)
	{
		File dir = new File(System.getProperty("java.io.tmpdir"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("No temporary directory exists in this environment.");
		}
		return RaccoonBuilder.this.path(new File(dir, aName));
	}


	/**
	 * The database will be stored in a file in the user home directory with the name provided.
	 */
	public RaccoonBuilder pathInUserDir(String aName)
	{
		File dir = new File(System.getProperty("user.home"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("User diretory not found in this environment.");
		}
		return RaccoonBuilder.this.path(new File(dir, aName));
	}


	public RaccoonBuilder encryption(String aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction == null ? null : EncryptionFunction.valueOf(aEncryptionFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder encryption(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public RaccoonBuilder keyGeneration(String aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction == null ? null : KeyGenerationFunction.valueOf(aKeyGenerationFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder keyGeneration(KeyGenerationFunction aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction;
		return this;
	}


	public RaccoonBuilder cipherMode(String aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction == null ? null : CipherModeFunction.valueOf(aCipherModeFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder cipherMode(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public RaccoonBuilder password(char[] aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.clone();
		return this;
	}


	public RaccoonBuilder password(byte[] aPassword)
	{
		if (aPassword == null)
		{
			mPassword = null;
		}
		else
		{
			char[] tmp = new char[aPassword.length];
			for (int i = 0; i < aPassword.length; i++)
			{
				tmp[i] = (char)aPassword[i];
			}
			mPassword = tmp;
		}
		return this;
	}


	public RaccoonBuilder password(String aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.toCharArray();
		return this;
	}


	public RaccoonBuilder compressor(CompressorAlgorithm aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm;
		return this;
	}


	public RaccoonBuilder compressor(String aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm == null ? null : CompressorAlgorithm.valueOf(aCompressorAlgorithm.toUpperCase());
		return this;
	}


	/**
	 * Sets the encryption key generator iteration count. Default 1000.
	 */
	public RaccoonBuilder keyIterations(int aIterationCount)
	{
		mKeyGeneratorIterationCount = aIterationCount;
		return this;
	}
}
