package org.terifan.raccoon;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.terifan.raccoon.blockdevice.RaccoonIOException;
import org.terifan.raccoon.blockdevice.compressor.CompressorAlgorithm;
import org.terifan.raccoon.blockdevice.managed.ManagedBlockDevice;
import org.terifan.raccoon.blockdevice.secure.AccessCredentials;
import org.terifan.raccoon.blockdevice.secure.CipherModeFunction;
import org.terifan.raccoon.blockdevice.secure.EncryptionFunction;
import org.terifan.raccoon.blockdevice.secure.KeyGenerationFunction;
import org.terifan.raccoon.blockdevice.secure.SecureBlockDevice;
import org.terifan.raccoon.blockdevice.storage.BlockStorage;
import org.terifan.raccoon.blockdevice.storage.MemoryBlockStorage;


public class RaccoonBuilder
{
	private Object mTarget;
	private EncryptionFunction mEncryptionFunction;
	private KeyGenerationFunction mKeyGenerationFunction;
	private CipherModeFunction mCipherModeFunction;
	private CompressorAlgorithm mCompressorAlgorithm;
	private char[] mPassword;
	private int mKeyGeneratorIterationCount;
	private int mBlockSize;
//	private SyncMode mSyncMode;


	public RaccoonBuilder()
	{
		mBlockSize = 4096;
		mKeyGeneratorIterationCount = 1000_000;
		mEncryptionFunction = EncryptionFunction.AES;
		mCipherModeFunction = CipherModeFunction.XTS;
		mKeyGenerationFunction = KeyGenerationFunction.SHA512;
		mCompressorAlgorithm = CompressorAlgorithm.LZJB;
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary folder with a random name. File will be deleted on exit.
	 */
	public static RaccoonBuilder temporaryFile()
	{
		return temporaryFile(null);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the system temporary folder with provided name. File will be deleted on exit.
	 */
	public static RaccoonBuilder temporaryFile(String aName)
	{
		try
		{
			Path path = Files.createTempFile(aName, ".rdb");
			path.toFile().deleteOnExit();
			return new RaccoonBuilder().withTarget(path);
		}
		catch (IOException e)
		{
			throw new RaccoonIOException("Failed to create file in temporary folder", e);
		}
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file in the user home folder with provided relative path.
	 */
	public static RaccoonBuilder userFile(String aPath)
	{
		File dir = new File(System.getProperty("user.home"));
		if (!dir.exists())
		{
			throw new IllegalArgumentException("User diretory not found in this environment.");
		}
		Path path = new File(dir, aPath).toPath();
		try
		{
			Files.createDirectories(path.getParent());
		}
		catch (IOException e)
		{
			throw new RaccoonIOException("Failed to create path: " + path);
		}
		return new RaccoonBuilder().withTarget(path);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public static RaccoonBuilder path(Path aPath)
	{
		return new RaccoonBuilder().withTarget(aPath);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public static RaccoonBuilder path(String aPath)
	{
		return new RaccoonBuilder().withTarget(aPath);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a file.
	 */
	public static RaccoonBuilder path(File aPath)
	{
		return new RaccoonBuilder().withTarget(aPath);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a low level BlockStorage.
	 */
	public static RaccoonBuilder storage(BlockStorage aBlockStorage)
	{
		return new RaccoonBuilder().withTarget(aBlockStorage);
	}


	/**
	 * Return a RaccoonBuilder with a target set to a BlockDevice that exists in RAM only.
	 *
	 * NOTE: changing the BlockSize will reset the block storage.
	 */
	public static RaccoonBuilder memory()
	{
		return new RaccoonBuilder().withTarget(new MemoryBlockStorage());
	}


	/**
	 * Sets the target location or storage for this database.
	 * <p>
	 * Create a reusable memory database:
	 * <pre>
	 * RaccoonBuilder builder = new RaccoonBuilder().target(new MemoryBlockStorage(512));
	 * try (RaccoonDatabase db = builder.get())
	 * {
	 *    db.getCollection("data").saveOne(Document.of("value:1"));
	 * }
	 * try (RaccoonDatabase db = builder.get())
	 * {
	 *    db.getCollection("data").forEach(System.out::println);
	 * }
	 * </pre>
	 */
	public RaccoonBuilder withTarget(Object aTarget)
	{
		mTarget = aTarget;
		return this;
	}


	/**
	 * Open or create a RaccoonDatabase with the properties in the builder.
	 */
	public RaccoonDatabase get()
	{
		return get(DatabaseOpenOption.CREATE);
	}


	/**
	 * Open or create a RaccoonDatabase with the properties in the builder.
	 */
	public RaccoonDatabase get(DatabaseOpenOption aDatabaseOpenOption)
	{
		if (mTarget == null)
		{
			throw new IllegalArgumentException("Target is null.");
		}

		AccessCredentials ac = mPassword == null ? null : new AccessCredentials(mPassword, mEncryptionFunction, mKeyGenerationFunction, mCipherModeFunction).setIterationCount(mKeyGeneratorIterationCount);

		if (mTarget instanceof MemoryBlockStorage v)
		{
			v.setBlockSize(mBlockSize);
			return new RaccoonDatabase(new ManagedBlockDevice(v), aDatabaseOpenOption, ac);
		}
		if (mTarget instanceof File v)
		{
			return new RaccoonDatabase(v.toPath(), aDatabaseOpenOption, ac);
		}
		if (mTarget instanceof String v)
		{
			return new RaccoonDatabase(Paths.get(v), aDatabaseOpenOption, ac);
		}
		if (mTarget instanceof Path v)
		{
			return new RaccoonDatabase(v, aDatabaseOpenOption, ac);
		}
		if (mTarget instanceof BlockStorage v)
		{
			if (ac == null)
			{
				return new RaccoonDatabase(new ManagedBlockDevice(v), aDatabaseOpenOption, null);
			}
			return new RaccoonDatabase(new ManagedBlockDevice(new SecureBlockDevice(ac, v)), aDatabaseOpenOption, null);
		}

		throw new IllegalArgumentException("Unsupported target specified: " + mTarget.getClass());
	}


	public RaccoonBuilder withEncryption(String aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction == null ? null : EncryptionFunction.valueOf(aEncryptionFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder withEncryption(EncryptionFunction aEncryptionFunction)
	{
		mEncryptionFunction = aEncryptionFunction;
		return this;
	}


	public RaccoonBuilder withKeyGeneration(String aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction == null ? null : KeyGenerationFunction.valueOf(aKeyGenerationFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder withKeyGeneration(KeyGenerationFunction aKeyGenerationFunction)
	{
		mKeyGenerationFunction = aKeyGenerationFunction;
		return this;
	}


	public RaccoonBuilder withCipherMode(String aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction == null ? null : CipherModeFunction.valueOf(aCipherModeFunction.toUpperCase());
		return this;
	}


	public RaccoonBuilder withCipherMode(CipherModeFunction aCipherModeFunction)
	{
		mCipherModeFunction = aCipherModeFunction;
		return this;
	}


	public RaccoonBuilder withPassword(char[] aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.clone();
		return this;
	}


	public RaccoonBuilder withPassword(byte[] aPassword)
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


	public RaccoonBuilder withPassword(String aPassword)
	{
		mPassword = aPassword == null ? null : aPassword.toCharArray();
		return this;
	}


	public RaccoonBuilder withCompressor(CompressorAlgorithm aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm;
		return this;
	}


	public RaccoonBuilder withCompressor(String aCompressorAlgorithm)
	{
		mCompressorAlgorithm = aCompressorAlgorithm == null ? null : CompressorAlgorithm.valueOf(aCompressorAlgorithm.toUpperCase());
		return this;
	}


	/**
	 * Sets the encryption key generator iteration count. Default is 1,000,000.
	 */
	public RaccoonBuilder withKeyIterations(int aIterationCount)
	{
		mKeyGeneratorIterationCount = aIterationCount;
		return this;
	}


	/**
	 * Sets the size of a storage block in the block device. This value should be same or a multiple of the underlaying file systems sector size.
	 *
	 * @param aBlockSize must be power of 2, and between 512 and 65536, default is 4096.
	 */
	public RaccoonBuilder withBlockSize(int aBlockSize)
	{
		if (aBlockSize < 512 || aBlockSize > 65536 || (aBlockSize & (aBlockSize - 1)) != 0)
		{
			throw new IllegalArgumentException("Illegal block size: " + aBlockSize);
		}

		mBlockSize = aBlockSize;
		return this;
	}
}
