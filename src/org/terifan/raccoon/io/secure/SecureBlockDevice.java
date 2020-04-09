package org.terifan.raccoon.io.secure;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import org.terifan.raccoon.io.physical.FileAlreadyOpenException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.messagedigest.MurmurHash3;
import org.terifan.security.cryptography.SecretKey;
import org.terifan.raccoon.util.Log;
import static java.util.Arrays.fill;
import org.terifan.raccoon.DatabaseIOException;
import org.terifan.security.cryptography.CipherMode;
import org.terifan.security.cryptography.ISAAC;
import static org.terifan.raccoon.util.ByteArrayUtil.*;


/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device. The blocks at index 0 and 1
 * contain a boot blocks which store the secret encryption keys used to encrypt all other blocks. All read and write operations
 * offset the index to ensure the boot blocks can never be read/written.
 */
public final class SecureBlockDevice implements IPhysicalBlockDevice, AutoCloseable
{
	private final static int BOOT_BLOCK_COUNT = 2;
	private final static int RESERVED_BLOCKS = BOOT_BLOCK_COUNT;
	private final static int SALT_SIZE = 256;
	private final static int PAYLOAD_SIZE = 256;
	private final static int HEADER_SIZE = 4;
	private final static int KEY_SIZE_BYTES = 32;
	private final static int IV_SIZE = 16;
	private final static int KEY_POOL_SIZE = KEY_SIZE_BYTES + 3 * KEY_SIZE_BYTES + 3 * IV_SIZE;
	private final static int CHECKSUM_SEED = 0x2fc8d359; // (random number)

	private transient IPhysicalBlockDevice mBlockDevice;
	private transient CipherImplementation mCipherImplementation;


	/**
	 *
	 * Note: the AccessCredentials object provides the SecureBlockDevice with cryptographic keys and is slow to instantiate.
	 * Reuse the same AccessCredentials instance for a single password when opening multiple SecureBlockDevices.
	 */
	private SecureBlockDevice()
	{
	}


	public static SecureBlockDevice create(AccessCredentials aAccessCredentials, IPhysicalBlockDevice aBlockDevice)
	{
		if (aBlockDevice == null)
		{
			throw new IllegalArgumentException("BlockDevice is null");
		}
		if (aAccessCredentials == null)
		{
			throw new IllegalArgumentException("AccessCredentials is null");
		}
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("Block size must be 512 bytes or more.");
		}

		SecureBlockDevice device = new SecureBlockDevice();
		device.mBlockDevice = aBlockDevice;

		Log.i("create boot block");
		Log.inc();

		byte[] payload = new byte[PAYLOAD_SIZE];

		for (;;)
		{
			// create the secret keys
			try
			{
				byte[] raw = new byte[PAYLOAD_SIZE * 8];

				SecureRandom rnd = SecureRandom.getInstanceStrong();
				rnd.nextBytes(raw);

				for (int dst = 0, src = 0; dst < payload.length; dst++)
				{
					for (long i = System.nanoTime() & 7; i >= 0; i--)
					{
						payload[dst] ^= raw[src++];
					}
				}
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new IllegalStateException(e);
			}

			if (createImpl(aAccessCredentials, device, payload, 0L) && createImpl(aAccessCredentials, device, payload, 1L))
			{
				break;
			}
		}

		// cleanup
		Arrays.fill(payload, (byte)0);

		Log.dec();

		return device;
	}


	private static boolean createImpl(AccessCredentials aAccessCredentials, SecureBlockDevice aDevice, byte[] aPayload, long aBlockIndex)
	{
		byte[] blockData = createBootBlock(aAccessCredentials, aPayload, aBlockIndex, aDevice.mBlockDevice.getBlockSize());

		CipherImplementation cipher = readBootBlock(aAccessCredentials, blockData, aBlockIndex, true);

		if (cipher == null)
		{
			// TODO: improve
			return false;
		}

		aDevice.mCipherImplementation = cipher;
		aDevice.mBlockDevice.writeBlock(aBlockIndex, blockData, 0, blockData.length, new long[2]);

		return true;
	}


	private static byte[] createBootBlock(AccessCredentials aAccessCredentials, byte[] aPayload, long aBlockIndex, int aBlockSize)
	{
		byte[] salt = new byte[SALT_SIZE];
		byte[] padding = new byte[aBlockSize - SALT_SIZE - PAYLOAD_SIZE];

		// padding and salt
		ISAAC.PRNG.nextBytes(padding);
		ISAAC.PRNG.nextBytes(salt);

		// compute checksum
		int checksum = computeChecksum(salt, aPayload);

		// update header
		putInt(aPayload, 0, checksum);

		// create user key
		byte[] userKeyPool = aAccessCredentials.generateKeyPool(aAccessCredentials.getKeyGeneratorFunction(), salt, KEY_POOL_SIZE);

		// encrypt payload
		byte[] payload = aPayload.clone();

		CipherImplementation cipher = new CipherImplementation(aAccessCredentials.getCipherModeFunction(), aAccessCredentials.getEncryptionFunction(), userKeyPool, 0, PAYLOAD_SIZE);
		cipher.encrypt(aBlockIndex, payload, 0, PAYLOAD_SIZE, new long[2]);

		// assemble output buffer
		byte[] blockData = new byte[aBlockSize];
		System.arraycopy(salt, 0, blockData, 0, SALT_SIZE);
		System.arraycopy(payload, 0, blockData, SALT_SIZE, PAYLOAD_SIZE);
		System.arraycopy(padding, 0, blockData, SALT_SIZE + PAYLOAD_SIZE, padding.length);

		return blockData;
	}


	public static SecureBlockDevice open(AccessCredentials aAccessCredentials, IPhysicalBlockDevice aBlockDevice)
	{
		return open(aAccessCredentials, aBlockDevice, 0);
	}


	public static SecureBlockDevice open(AccessCredentials aAccessCredentials, IPhysicalBlockDevice aBlockDevice, long aBlockIndex)
	{
		if (aBlockDevice == null)
		{
			throw new IllegalArgumentException("BlockDevice is null");
		}
		if (aAccessCredentials == null)
		{
			throw new IllegalArgumentException("AccessCredentials is null");
		}
		if (aBlockDevice.getBlockSize() < 512)
		{
			throw new IllegalArgumentException("Block size is less than 512 bytes");
		}

		SecureBlockDevice device = new SecureBlockDevice();
		device.mBlockDevice = aBlockDevice;

		Log.i("open boot block #%s", aBlockIndex);
		Log.inc();

		byte[] blockData = new byte[device.mBlockDevice.getBlockSize()];

		try
		{
			device.mBlockDevice.readBlock(aBlockIndex, blockData, 0, blockData.length, new long[2]);
		}
		catch (Exception e)
		{
			throw new FileAlreadyOpenException("Error reading boot block. Database file might already be open?", e);
		}

		device.mCipherImplementation = readBootBlock(aAccessCredentials, blockData, aBlockIndex, false);

		if (device.mCipherImplementation != null)
		{
			return device;
		}

		return null;
	}


	private static CipherImplementation readBootBlock(AccessCredentials aAccessCredentials, byte[] aBlockData, long aBlockIndex, boolean aVerifyFunctions)
	{
		// extract the salt and payload
		byte[] salt = getBytes(aBlockData, 0, SALT_SIZE);
		byte[] payload = getBytes(aBlockData, SALT_SIZE, PAYLOAD_SIZE);

		for (KeyGenerationFunction keyGenerator : KeyGenerationFunction.values())
		{
			// create a user key using the key generator
			byte[] userKeyPool = aAccessCredentials.generateKeyPool(keyGenerator, salt, KEY_POOL_SIZE);

			// decode boot block using all available ciphers
			for (EncryptionFunction encryption : EncryptionFunction.values())
			{
				for (CipherModeFunction cipherMode : CipherModeFunction.values())
				{
					byte[] payloadCopy = payload.clone();

					// decrypt payload using the user key
					CipherImplementation cipher = new CipherImplementation(cipherMode, encryption, userKeyPool, 0, PAYLOAD_SIZE);
					cipher.decrypt(aBlockIndex, payloadCopy, 0, PAYLOAD_SIZE, new long[2]);

					// read header
					int expectedChecksum = getInt(payloadCopy, 0);

					// verify checksum of boot block
					if (expectedChecksum == computeChecksum(salt, payloadCopy))
					{
						Log.dec();

						// when a boot block is created it's also verified
						if (aVerifyFunctions && (aAccessCredentials.getKeyGeneratorFunction() != keyGenerator || aAccessCredentials.getEncryptionFunction() != encryption))
						{
							System.out.println("hash collision in boot block");

							// a checksum collision has occured!
							return null;
						}

						// create the cipher used to encrypt data blocks
						return new CipherImplementation(cipherMode, encryption, payloadCopy, HEADER_SIZE, aBlockData.length);
					}
				}
			}
		}

		Log.w("incorrect password or not a secure BlockDevice");
		Log.dec();

		return null;
	}


	private static int computeChecksum(byte[] aSalt, byte[] aPayloadCopy)
	{
		return MurmurHash3.hash32(aSalt, CHECKSUM_SEED) ^ MurmurHash3.hash32(aPayloadCopy, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);
	}


	@Override
	public void writeBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long[] aIV)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipherImplementation.encrypt(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aIV);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, new long[2]); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long[] aIV)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, new long[2]); // block key is used by this blockdevice and not passed to lower levels

		mCipherImplementation.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV);

		Log.dec();
	}


	public void writeBlockWithIV(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength)
	{
		// the buffer must end with 16 zero bytes reserved for the IV
		assert getLong(aBuffer, aBuffer.length - 16) == 0;
		assert getLong(aBuffer, aBuffer.length - 8) == 0;

		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		long[] iv =
		{
			ISAAC.PRNG.nextLong(),
			ISAAC.PRNG.nextLong()
		};

		putLong(workBuffer, workBuffer.length - 16, iv[0]);
		putLong(workBuffer, workBuffer.length - 8, iv[1]);

		mCipherImplementation.encrypt(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength - 16, iv);

		mCipherImplementation.mTweakCipher.engineEncryptBlock(workBuffer, workBuffer.length - 16, workBuffer, workBuffer.length - 16);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, new long[2]); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	public void readBlockWithIV(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength)
	{
		if (aBlockIndex < 0)
		{
			throw new DatabaseIOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, new long[2]); // block key is used by this blockdevice and not passed to lower levels

		mCipherImplementation.mTweakCipher.engineDecryptBlock(aBuffer, aBuffer.length - 16, aBuffer, aBuffer.length - 16);

		long[] iv =
		{
			getLong(aBuffer, aBuffer.length - 16),
			getLong(aBuffer, aBuffer.length - 8)
		};

		mCipherImplementation.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength - 16, iv);

		Arrays.fill(aBuffer, aBuffer.length - 16, aBuffer.length, (byte)0);

		Log.dec();
	}


	@Override
	public int getBlockSize()
	{
		return mBlockDevice.getBlockSize();
	}


	@Override
	public long length()
	{
		return mBlockDevice.length() - RESERVED_BLOCKS;
	}


	@Override
	public void commit(boolean aMetadata)
	{
		mBlockDevice.commit(aMetadata);
	}


	@Override
	public void setLength(long aLength)
	{
		mBlockDevice.setLength(aLength + RESERVED_BLOCKS);
	}


	@Override
	public void close()
	{
		if (mCipherImplementation != null)
		{
			mCipherImplementation.reset();
			mCipherImplementation = null;
		}

		if (mBlockDevice != null)
		{
			mBlockDevice.close();
			mBlockDevice = null;
		}
	}


	protected void validateBootBlocks(AccessCredentials aAccessCredentials)
	{
		byte[] original = new byte[mBlockDevice.getBlockSize()];
		byte[] encypted = original.clone();

		for (int i = 0; i < BOOT_BLOCK_COUNT; i++)
		{
			CipherImplementation cipher;

			try
			{
				SecureBlockDevice tmp = SecureBlockDevice.open(aAccessCredentials, mBlockDevice, i);
				cipher = tmp.mCipherImplementation;
			}
			catch (Exception e)
			{
				throw new DatabaseIOException("Failed to read boot block " + i);
			}

			if (i == 0)
			{
				cipher.encrypt(0, encypted, 0, original.length, new long[2]);
			}
			else
			{
				byte[] decrypted = encypted.clone();

				cipher.decrypt(0, decrypted, 0, original.length, new long[2]);

				if (!Arrays.equals(original, decrypted))
				{
					throw new DatabaseIOException("Boot blocks are incompatible");
				}
			}
		}
	}


	private static final class CipherImplementation
	{
		private transient final long[][] mMasterIV;
		private transient final BlockCipher[] mCiphers;
		private transient final CipherMode mCipherMode;
		private transient final int mUnitSize;
		private transient BlockCipher mTweakCipher;


		public CipherImplementation(final CipherModeFunction aCipherModeFunction, final EncryptionFunction aEncryptionFunction, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitSize)
		{
			mUnitSize = aUnitSize;
			mCipherMode = aCipherModeFunction.newInstance();
			mCiphers = aEncryptionFunction.newInstance();
			mTweakCipher = aEncryptionFunction.newTweakInstance();
			mMasterIV = new long[mCiphers.length][2];

			int offset = aKeyPoolOffset;

			mTweakCipher.engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
			offset += KEY_SIZE_BYTES;

			for (int i = 0; i < 3; i++)
			{
				if (i < mCiphers.length)
				{
					mCiphers[i].engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
				}
				offset += KEY_SIZE_BYTES;
			}

			for (int i = 0; i < 3; i++)
			{
				if (i < mMasterIV.length)
				{
					mMasterIV[i][0] = getLong(aKeyPool, offset + 0);
					mMasterIV[i][1] = getLong(aKeyPool, offset + 8);
				}
				offset += IV_SIZE;
			}
		}


		public void encrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long[] aBlockIV)
		{
			for (int i = 0; i < mCiphers.length; i++)
			{
				mCipherMode.encrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, Math.min(mUnitSize, aLength), mMasterIV[i], aBlockIV, mTweakCipher);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long[] aBlockIV)
		{
			for (int i = mCiphers.length; --i >= 0;)
			{
				mCipherMode.decrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, Math.min(mUnitSize, aLength), mMasterIV[i], aBlockIV, mTweakCipher);
			}
		}


		private void reset()
		{
			for (BlockCipher cipher : mCiphers)
			{
				if (cipher != null)
				{
					cipher.engineReset();
				}
			}

			for (int i = 0; i < mMasterIV.length; i++)
			{
				mMasterIV[i][0] = mMasterIV[i][1] = 0L;
			}

			fill(mCiphers, null);
		}
	};
}
