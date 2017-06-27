package org.terifan.raccoon.io.secure;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.io.physical.FileAlreadyOpenException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.messagedigest.MurmurHash3;
import org.terifan.security.cryptography.SecretKey;
import org.terifan.raccoon.util.Log;
import static java.util.Arrays.fill;
import org.terifan.security.cryptography.BitScrambler;
import org.terifan.security.cryptography.CipherMode;


// Boot block layout:
// 256 salt (random, plaintext)
// 256 payload (encrypted with user key)
//       4 checksum (salt + key pool + payload padding)
//     208 key pool
//          96 ciper keys (3 x 32)
//          48 cipher iv (3 x 16)
//      44 payload padding
//   n padding (random, plaintext)

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
	private final static int KEY_POOL_SIZE = 3 * KEY_SIZE_BYTES + 3 * IV_SIZE;
	private final static int SCRAMBLE_KEY_OFFSET = KEY_POOL_SIZE;
	private final static int USER_KEY_POOL_SIZE = SCRAMBLE_KEY_OFFSET + 2 * 8;
	private final static int CHECKSUM_SEED = 0x2fc8d359; // (random number)

	private transient IPhysicalBlockDevice mBlockDevice;
	private transient CipherImplementation mCipher;


	/**
	 *
	 * Note: the AccessCredentials object provides the SecureBlockDevice with cryptographic keys and is slow to instantiate.
	 * Reuse the same AccessCredentials instance for a single password when opening multiple SecureBlockDevices.
	 */
	private SecureBlockDevice()
	{
	}


	public static SecureBlockDevice create(AccessCredentials aAccessCredentials, IPhysicalBlockDevice aBlockDevice) throws IOException
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

		// create the secret keys
		try
		{
			byte[] raw = new byte[PAYLOAD_SIZE * 10];

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

		createImpl(aAccessCredentials, device, payload, 0L);
		createImpl(aAccessCredentials, device, payload, 1L);

		// cleanup
		Arrays.fill(payload, (byte)0);

		Log.dec();

		return device;
	}


	private static void createImpl(AccessCredentials aAccessCredentials, SecureBlockDevice aDevice, byte[] aPayload, long aBlockIndex) throws IOException
	{
		byte[] blockData = createBootBlock(aAccessCredentials, aPayload, aBlockIndex, aDevice.mBlockDevice.getBlockSize());

		CipherImplementation cipher = readBootBlock(aAccessCredentials, blockData, aBlockIndex, true);

		if (cipher == null)
		{
			// TODO: improve
			createImpl(aAccessCredentials, aDevice, aPayload, aBlockIndex);
			return;
		}

		aDevice.mCipher = cipher;
		aDevice.mBlockDevice.writeBlock(aBlockIndex, blockData, 0, blockData.length, 0L, 0L);
	}


	private static byte[] createBootBlock(AccessCredentials aAccessCredentials, byte[] aPayload, long aBlockIndex, int aBlockSize) throws IOException
	{
		byte[] salt = new byte[SALT_SIZE];
		byte[] padding = new byte[aBlockSize - SALT_SIZE - PAYLOAD_SIZE];

		// padding and salt
		Random rand = new Random();
		rand.nextBytes(padding);
		rand.nextBytes(salt);

		// compute checksum
		int checksum = computeChecksum(salt, aPayload);

		// update header
		putInt(aPayload, 0, checksum);

		// create user key
		byte[] userKeyPool = aAccessCredentials.generateKeyPool(aAccessCredentials.getKeyGeneratorFunction(), salt, USER_KEY_POOL_SIZE);

		// encrypt payload
		byte[] payload = aPayload.clone();
		long scrambleKey0 = getLong(userKeyPool, SCRAMBLE_KEY_OFFSET);
		long scrambleKey1 = getLong(userKeyPool, SCRAMBLE_KEY_OFFSET + 8);

		BitScrambler.scramble(scrambleKey0, payload);

		CipherImplementation cipher = new CipherImplementation(aAccessCredentials.getCipherModeFunction(), aAccessCredentials.getEncryptionFunction(), userKeyPool, 0, PAYLOAD_SIZE);
		cipher.encrypt(aBlockIndex, payload, 0, PAYLOAD_SIZE, 0L, 0L);

		BitScrambler.scramble(scrambleKey1, payload);

		// assemble output buffer
		byte[] blockData = new byte[aBlockSize];
		System.arraycopy(salt, 0, blockData, 0, SALT_SIZE);
		System.arraycopy(payload, 0, blockData, SALT_SIZE, PAYLOAD_SIZE);
		System.arraycopy(padding, 0, blockData, SALT_SIZE + PAYLOAD_SIZE, padding.length);

		return blockData;
	}


	public static SecureBlockDevice open(IPhysicalBlockDevice aBlockDevice, AccessCredentials aAccessCredentials) throws IOException
	{
		return open(aBlockDevice, aAccessCredentials, 0);
	}


	public static SecureBlockDevice open(IPhysicalBlockDevice aBlockDevice, AccessCredentials aAccessCredentials, long aBlockIndex) throws IOException
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

		byte [] blockData = new byte[device.mBlockDevice.getBlockSize()];

		try
		{
			device.mBlockDevice.readBlock(aBlockIndex, blockData, 0, blockData.length, 0L, 0L);
		}
		catch (Exception e)
		{
			throw new FileAlreadyOpenException("Error reading boot block. Database file might already be open?", e);
		}

		device.mCipher = readBootBlock(aAccessCredentials, blockData, aBlockIndex, false);

		if (device.mCipher != null)
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
			byte[] userKeyPool = aAccessCredentials.generateKeyPool(keyGenerator, salt, USER_KEY_POOL_SIZE);

			long scrambleKey0 = getLong(userKeyPool, SCRAMBLE_KEY_OFFSET + 8);
			long scrambleKey1 = getLong(userKeyPool, SCRAMBLE_KEY_OFFSET);

			// decode boot block using all available ciphers
			for (EncryptionFunction encryption : EncryptionFunction.values())
			{
				for (CipherModeFunction cipherMode : CipherModeFunction.values())
				{
					byte[] payloadCopy = payload.clone();

					BitScrambler.unscramble(scrambleKey0, payloadCopy);

					// decrypt payload using the user key
					CipherImplementation cipher = new CipherImplementation(cipherMode, encryption, userKeyPool, 0, PAYLOAD_SIZE);
					cipher.decrypt(aBlockIndex, payloadCopy, 0, PAYLOAD_SIZE, 0L, 0L);

					BitScrambler.unscramble(scrambleKey1, payloadCopy);

					// read header
					int expectedChecksum = getInt(payloadCopy, 0);

					// verify checksum of boot block
					if (expectedChecksum == computeChecksum(salt, payloadCopy))
					{
						Log.dec();

						// when a boot block is created it's also verified
						if (aVerifyFunctions && (aAccessCredentials.getKeyGeneratorFunction() != keyGenerator || aAccessCredentials.getEncryptionFunction() != encryption))
						{
							System.err.println("hash collision in boot block");

							// a hash collision has occured!
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
		return MurmurHash3.hash_x86_32(aSalt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(aPayloadCopy, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);
	}


	@Override
	public void writeBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aIV0, final long aIV1) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipher.encrypt(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, 0L, 0L); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aIV0, final long aIV1) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L, 0L); // block key is used by this blockdevice and not passed to lower levels

		mCipher.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aIV0, aIV1);

		Log.dec();
	}


	@Override
	public int getBlockSize() throws IOException
	{
		return mBlockDevice.getBlockSize();
	}


	@Override
	public long length() throws IOException
	{
		return mBlockDevice.length() - RESERVED_BLOCKS;
	}


	@Override
	public void commit(boolean aMetadata) throws IOException
	{
		mBlockDevice.commit(aMetadata);
	}


	@Override
	public void setLength(long aLength) throws IOException
	{
		mBlockDevice.setLength(aLength + RESERVED_BLOCKS);
	}


	@Override
	public void close() throws IOException
	{
		if (mCipher != null)
		{
			mCipher.reset();
			mCipher = null;
		}

		if (mBlockDevice != null)
		{
			mBlockDevice.close();
			mBlockDevice = null;
		}
	}


	private static int getInt(byte [] aBuffer, int aPosition)
	{
		return    ((aBuffer[aPosition++] & 255) << 24)
				+ ((aBuffer[aPosition++] & 255) << 16)
				+ ((aBuffer[aPosition++] & 255) <<  8)
				+ ((aBuffer[aPosition  ] & 255)      );
	}


	private static void putInt(byte [] aBuffer, int aPosition, int aValue)
	{
		aBuffer[aPosition++] = (byte)(aValue >>> 24);
		aBuffer[aPosition++] = (byte)(aValue >>  16);
		aBuffer[aPosition++] = (byte)(aValue >>   8);
		aBuffer[aPosition  ] = (byte)(aValue       );
	}


	private static byte[] getBytes(byte[] aBuffer, int aOffset, int aLength)
	{
		byte[] buf = new byte[aLength];
		System.arraycopy(aBuffer, aOffset, buf, 0, aLength);
		return buf;
	}


//	private static void putLong(byte[] aBuffer, int aOffset, long aValue)
//	{
//		aBuffer[aOffset + 7] = (byte)(aValue >>> 0);
//		aBuffer[aOffset + 6] = (byte)(aValue >>> 8);
//		aBuffer[aOffset + 5] = (byte)(aValue >>> 16);
//		aBuffer[aOffset + 4] = (byte)(aValue >>> 24);
//		aBuffer[aOffset + 3] = (byte)(aValue >>> 32);
//		aBuffer[aOffset + 2] = (byte)(aValue >>> 40);
//		aBuffer[aOffset + 1] = (byte)(aValue >>> 48);
//		aBuffer[aOffset + 0] = (byte)(aValue >>> 56);
//	}


	private static long getLong(byte[] aBuffer, int aOffset)
	{
		return ((255 & aBuffer[aOffset + 7]))
			+ ((255 & aBuffer[aOffset + 6]) << 8)
			+ ((255 & aBuffer[aOffset + 5]) << 16)
			+ ((long)(255 & aBuffer[aOffset + 4]) << 24)
			+ ((long)(255 & aBuffer[aOffset + 3]) << 32)
			+ ((long)(255 & aBuffer[aOffset + 2]) << 40)
			+ ((long)(255 & aBuffer[aOffset + 1]) << 48)
			+ ((long)(255 & aBuffer[aOffset + 0]) << 56);
	}


	protected void validateBootBlocks(AccessCredentials aAccessCredentials) throws IOException
	{
		byte[] original = new byte[mBlockDevice.getBlockSize()];
		byte[] encypted = original.clone();

		for (int i = 0; i < BOOT_BLOCK_COUNT; i++)
		{
			CipherImplementation cipher;

			try
			{
				SecureBlockDevice tmp = SecureBlockDevice.open(mBlockDevice, aAccessCredentials, i);
				cipher = tmp.mCipher;
			}
			catch (Exception e)
			{
				throw new IOException("Failed to read boot block " + i);
			}

			if (i == 0)
			{
				cipher.encrypt(0, encypted, 0, original.length, 0L, 0L);
			}
			else
			{
				byte[] decrypted = encypted.clone();

				cipher.decrypt(0, decrypted, 0, original.length, 0L, 0L);

				if (!Arrays.equals(original, decrypted))
				{
					throw new IOException("Boot blocks are incompatible");
				}
			}
		}
	}


	private static final class CipherImplementation
	{
		private transient final long [][] mIV;
		private transient final BlockCipher[] mCiphers;
		private transient final CipherMode mCipherMode;
		private transient final int mUnitSize;


		public CipherImplementation(final CipherModeFunction aCipherModeFunction, final EncryptionFunction aEncryptionFunction, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitSize)
		{
			mUnitSize = aUnitSize;
			mCipherMode = aCipherModeFunction.newInstance();
			mCiphers = aEncryptionFunction.newInstance();
			mIV = new long[mCiphers.length][2];

			int offset = aKeyPoolOffset;

			for (BlockCipher cipher : mCiphers)
			{
				cipher.engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
				offset += KEY_SIZE_BYTES;
			}

			for (int i = 0; i < mIV.length; i++)
			{
				mIV[i][0] = getLong(aKeyPool, offset + 0);
				mIV[i][1] = getLong(aKeyPool, offset + 8);
				offset += IV_SIZE;
			}
		}


		public void encrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aIV0, final long aIV1)
		{
			for (int i = 0; i < mCiphers.length; i++)
			{
				mCipherMode.encrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, mUnitSize, mIV[i], aIV0, aIV1);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aIV0, final long aIV1)
		{
			for (int i = mCiphers.length; --i >= 0; )
			{
				mCipherMode.decrypt(aBuffer, aOffset, aLength, mCiphers[i], aBlockIndex, mUnitSize, mIV[i], aIV0, aIV1);
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

			for (int i = 0; i < mIV.length; i++)
			{
				mIV[i][0] = mIV[i][1] = 0L;
			}

			fill(mCiphers, null);
		}
	};
}
