package org.terifan.raccoon.io.secure;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import org.terifan.raccoon.io.physical.FileAlreadyOpenException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.messagedigest.MurmurHash3;
import org.terifan.security.cryptography.SecretKey;
import org.terifan.security.cryptography.Serpent;
import org.terifan.security.cryptography.Twofish;
import org.terifan.raccoon.util.Log;
import static java.util.Arrays.fill;
import org.terifan.security.cryptography.BitScrambler;
import org.terifan.security.cryptography.XTSCipherMode;


// Boot block layout:
// 256 salt (random, plaintext)
// 256 payload (encrypted with user key)
//       4 header
//           4 checksum (salt + key pool + payload padding)
//     208 key pool
//          32 tweak cipher key (1 x 32)
//          96 ciper keys (3 x 32)
//          48 cipher iv (3 x 16)
//      44 payload padding
//   n padding (random, plaintext)

/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device. The block at index 0
 * contain a boot block which store the secret encryption key used to encrypt all other blocks. All read and write operations
 * offset the index to ensure the boot block can never be read/written.
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
	private final static int SCRAMBLE_KEY_OFFSET = KEY_POOL_SIZE;
	private final static int USER_KEY_POOL_SIZE = SCRAMBLE_KEY_OFFSET + 16;
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


	public static SecureBlockDevice create(IPhysicalBlockDevice aBlockDevice, AccessCredentials aAccessCredentials) throws IOException
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
			SecureRandom rnd = SecureRandom.getInstanceStrong();
			rnd.nextBytes(payload);
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new IllegalStateException(e);
		}

		// create the cipher used to encrypt data blocks
		device.mCipher = new CipherImplementation(aAccessCredentials.getEncryptionFunction(), payload, HEADER_SIZE, device.mBlockDevice.getBlockSize());

		device.createBootBlockImpl(aAccessCredentials, payload, 0L);
		device.createBootBlockImpl(aAccessCredentials, payload, 1L);

		// cleanup
		Arrays.fill(payload, (byte)0);

		Log.dec();

		return device;
	}


	private void createBootBlockImpl(AccessCredentials aAccessCredentials, byte[] aPayload, long aBlockIndex) throws IOException
	{
		byte[] salt = new byte[SALT_SIZE];
		byte[] padding = new byte[mBlockDevice.getBlockSize() - SALT_SIZE - PAYLOAD_SIZE];

		// padding and salt
		Random rand = new Random();
		rand.nextBytes(padding);
		rand.nextBytes(salt);

		// compute checksum
		int checksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(aPayload, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);

		// update header
		putInt(aPayload, 0, checksum);

		// create user key
		byte[] userKeyPool = aAccessCredentials.generateKeyPool(salt, USER_KEY_POOL_SIZE);

		// encrypt payload
		byte[] payload = aPayload.clone();

		BitScrambler.scramble(getLong(userKeyPool, SCRAMBLE_KEY_OFFSET + 8), payload);

		CipherImplementation cipher = new CipherImplementation(aAccessCredentials.getEncryptionFunction(), userKeyPool, 0, PAYLOAD_SIZE);
		cipher.encrypt(aBlockIndex, payload, 0, PAYLOAD_SIZE, 0L);

		BitScrambler.scramble(getLong(userKeyPool, SCRAMBLE_KEY_OFFSET), payload);

		// assemble output buffer
		byte[] blockData = new byte[mBlockDevice.getBlockSize()];
		System.arraycopy(salt, 0, blockData, 0, SALT_SIZE);
		System.arraycopy(payload, 0, blockData, SALT_SIZE, PAYLOAD_SIZE);
		System.arraycopy(padding, 0, blockData, SALT_SIZE + PAYLOAD_SIZE, padding.length);

		mBlockDevice.writeBlock(aBlockIndex, blockData, 0, mBlockDevice.getBlockSize(), 0L);
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

		// read boot block from disk
		int blockSize = device.mBlockDevice.getBlockSize();
		byte [] blockData = new byte[blockSize];

		try
		{
			device.mBlockDevice.readBlock(aBlockIndex, blockData, 0, blockSize, 0L);
		}
		catch (Exception e)
		{
			throw new FileAlreadyOpenException("Database file already open", e);
		}

		// extract the salt and payload
		byte[] salt = getBytes(blockData, 0, SALT_SIZE);
		byte[] payload = getBytes(blockData, SALT_SIZE, PAYLOAD_SIZE);

		for (KeyGenerationFunction keyGenerator : KeyGenerationFunction.values())
		{
			// create a user key using the key generator
			aAccessCredentials.setKeyGeneratorFunction(keyGenerator);
			byte[] userKeyPool = aAccessCredentials.generateKeyPool(salt, USER_KEY_POOL_SIZE);

			// decode boot block using all available ciphers
			for (EncryptionFunction ciphers : EncryptionFunction.values())
			{
				byte[] payloadCopy = payload.clone();

				BitScrambler.unscramble(getLong(userKeyPool, SCRAMBLE_KEY_OFFSET), payloadCopy);

				// decrypt payload using the user key
				CipherImplementation cipher = new CipherImplementation(ciphers, userKeyPool, 0, PAYLOAD_SIZE);
				cipher.decrypt(aBlockIndex, payloadCopy, 0, PAYLOAD_SIZE, 0L);

				BitScrambler.unscramble(getLong(userKeyPool, SCRAMBLE_KEY_OFFSET + 8), payloadCopy);

				// read header
				int expectedChecksum = getInt(payloadCopy, 0);

				// verify checksum of boot block
				int actualChecksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(payloadCopy, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);

				if (expectedChecksum == actualChecksum)
				{
					Log.dec();

					// create the cipher used to encrypt data blocks
					device.mCipher = new CipherImplementation(ciphers, payloadCopy, HEADER_SIZE, blockSize);

					return device;
				}
			}
		}

		Log.w("incorrect password or not a secure BlockDevice");
		Log.dec();

		return null;
	}


	@Override
	public void writeBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aTransactionId) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipher.encrypt(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aTransactionId);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aTransactionId) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.d("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		mCipher.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aTransactionId);

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


	private static final class CipherImplementation
	{
		private transient final byte [][] mIV = new byte[3][IV_SIZE];
		private transient final BlockCipher[] mCiphers;
		private transient final BlockCipher mTweakCipher;
		private transient final XTSCipherMode mCipherMode;
		private transient final int mUnitSize;


		public CipherImplementation(final EncryptionFunction aCiphers, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitSize)
		{
			mCipherMode = new XTSCipherMode();
			mUnitSize = aUnitSize;

			switch (aCiphers)
			{
				case AES:
					mCiphers = new BlockCipher[]{new AES()};
					mTweakCipher = new AES();
					break;
				case Twofish:
					mCiphers = new BlockCipher[]{new Twofish()};
					mTweakCipher = new Twofish();
					break;
				case Serpent:
					mCiphers = new BlockCipher[]{new Serpent()};
					mTweakCipher = new Serpent();
					break;
				case AESTwofish:
					mCiphers = new BlockCipher[]{new AES(), new Twofish()};
					mTweakCipher = new AES();
					break;
				case TwofishSerpent:
					mCiphers = new BlockCipher[]{new Twofish(), new Serpent()};
					mTweakCipher = new Twofish();
					break;
				case SerpentAES:
					mCiphers = new BlockCipher[]{new Serpent(), new AES()};
					mTweakCipher = new Serpent();
					break;
				case AESTwofishSerpent:
					mCiphers = new BlockCipher[]{new AES(), new Twofish(), new Serpent()};
					mTweakCipher = new AES();
					break;
				case TwofishAESSerpent:
					mCiphers = new BlockCipher[]{new Twofish(), new AES(), new Serpent()};
					mTweakCipher = new Twofish();
					break;
				case SerpentTwofishAES:
					mCiphers = new BlockCipher[]{new Serpent(), new Twofish(), new AES()};
					mTweakCipher = new Serpent();
					break;
				default:
					throw new IllegalStateException();
			}

			int offset = aKeyPoolOffset;

			mTweakCipher.engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
			offset += KEY_SIZE_BYTES;

			for (int j = 0; j < 3; j++)
			{
				if (j < mCiphers.length)
				{
					mCiphers[j].engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
				}
				offset += KEY_SIZE_BYTES;
			}

			for (int j = 0; j < 3; j++)
			{
				System.arraycopy(aKeyPool, offset, mIV[j], 0, IV_SIZE);
				offset += IV_SIZE;
			}

			if (offset != aKeyPoolOffset + KEY_POOL_SIZE)
			{
				throw new IllegalArgumentException("Bad offset: " + offset);
			}
		}


		public void encrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aBlockKey)
		{
			for (int i = 0; i < mCiphers.length; i++)
			{
				mCipherMode.encrypt(aBuffer, aOffset, aLength, mCiphers[i], mTweakCipher, aBlockIndex, mUnitSize, mIV[i], aBlockKey);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aBlockKey)
		{
			for (int i = mCiphers.length; --i >= 0; )
			{
				mCipherMode.decrypt(aBuffer, aOffset, aLength, mCiphers[i], mTweakCipher, aBlockIndex, mUnitSize, mIV[i], aBlockKey);
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

			mTweakCipher.engineReset();

			fill(mIV[0], (byte)0);
			fill(mIV[1], (byte)0);
			fill(mIV[2], (byte)0);
			fill(mCiphers, null);
		}
	};


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
				cipher.encrypt(0, encypted, 0, original.length, 0);
			}
			else
			{
				byte[] decrypted = encypted.clone();

				cipher.decrypt(0, decrypted, 0, original.length, 0);

				if (!Arrays.equals(original, decrypted))
				{
					throw new IOException("Boot blocks are incompatible");
				}
			}
		}
	}
}
