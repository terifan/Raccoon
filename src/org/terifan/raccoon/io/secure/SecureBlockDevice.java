package org.terifan.raccoon.io.secure;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import org.terifan.raccoon.io.physical.FileAlreadyOpenException;
import org.terifan.raccoon.io.physical.IPhysicalBlockDevice;
import org.terifan.security.cryptography.InvalidKeyException;
import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.CBCCipherMode;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.messagedigest.MurmurHash3;
import org.terifan.security.cryptography.SecretKey;
import org.terifan.security.cryptography.Serpent;
import org.terifan.security.cryptography.Twofish;
import org.terifan.raccoon.util.Log;
import static java.util.Arrays.fill;


// Boot block layout:
// 256 salt (random, plaintext)
// 256 payload (encrypted with user key)
//       8 header
//           4 signature
//           4 checksum (salt + key pool + payload padding)
//     208 key pool
//          32 tweak cipher key
//          96 ciper keys (3 x 32)
//          48 cipher iv (3 x 16)
//      40 payload padding
//   n padding (random, plaintext)

/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device. The block at index 0
 * contain a boot block which store the secret encryption key used to encrypt all other blocks. All read and write operations
 * offset the index to ensure the boot block can never be read/written.
 */
public final class SecureBlockDevice implements IPhysicalBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 1;
	private final static int SALT_SIZE = 256;
	private final static int IV_SIZE = 16;
	private final static int KEY_SIZE_BYTES = 32;
	private final static int HEADER_SIZE = 8;
	private final static int KEY_POOL_SIZE = KEY_SIZE_BYTES + 3 * KEY_SIZE_BYTES + 3 * IV_SIZE;
	private final static int ITERATION_COUNT = 10_000;
	private final static int PAYLOAD_SIZE = 256; // HEADER_SIZE + KEY_POOL_SIZE
	private final static int SIGNATURE = 0xf46a290c;
	private final static int CHECKSUM_SEED = 0x2fc8d359;

	private transient IPhysicalBlockDevice mBlockDevice;
	private transient CipherImplementation mCipher;


	/**
	 *
	 * Note: the AccessCredentials object provides the SecureBlockDevice with cryptographic keys and is slow to instantiate.
	 * Reuse the same AccessCredentials instance for a single password when opening multiple SecureBlockDevices.
	 */
	public SecureBlockDevice(IPhysicalBlockDevice aBlockDevice, AccessCredentials aAccessCredentials) throws IOException
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

		mBlockDevice = aBlockDevice;

		if (mBlockDevice.length() == 0)
		{
			createBootBlock(aAccessCredentials);
		}
		else
		{
			openBootBlock(aAccessCredentials);
		}
	}


	@Override
	public void writeBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.v("write block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipher.encrypt(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aBlockKey);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, workBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(final long aBlockIndex, final byte[] aBuffer, final int aBufferOffset, final int aBufferLength, final long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		Log.v("read block %d +%d", aBlockIndex, aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		mCipher.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

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
		mBlockDevice.setLength(aLength + 1);
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


	private void createBootBlock(final AccessCredentials aCredentials) throws IOException
	{
		Log.i("create boot block");
		Log.inc();

		byte[] salt = new byte[SALT_SIZE];
		byte[] payload = new byte[PAYLOAD_SIZE];
		byte[] padding = new byte[mBlockDevice.getBlockSize() - SALT_SIZE - PAYLOAD_SIZE];

		// create the secret keys
		SecureRandom rnd = new SecureRandom();
		rnd.nextBytes(payload);

		// create plain text random using Random function to not leak SecureRandom state
		Random rand = new Random();
		rand.nextBytes(padding);
		rand.nextBytes(salt);

		// compute checksum
		int checksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(payload, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);

		// update header
		putInt(payload, 0, SIGNATURE);
		putInt(payload, 4, checksum);

		// create the cipher used to encrypt data blocks
		mCipher = new CipherImplementation(aCredentials.getEncryptionFunction(), payload, HEADER_SIZE, mBlockDevice.getBlockSize());

		// create user key
		byte[] userKeyPool = aCredentials.generateKeyPool(salt, ITERATION_COUNT, KEY_POOL_SIZE);

		// encrypt payload
		CipherImplementation cipher = new CipherImplementation(aCredentials.getEncryptionFunction(), userKeyPool, 0, PAYLOAD_SIZE);
		cipher.encrypt(0, payload, 0, PAYLOAD_SIZE, 0L);

		// assemble output buffer
		byte[] blockData = new byte[mBlockDevice.getBlockSize()];
		System.arraycopy(salt, 0, blockData, 0, SALT_SIZE);
		System.arraycopy(payload, 0, blockData, SALT_SIZE, PAYLOAD_SIZE);
		System.arraycopy(padding, 0, blockData, SALT_SIZE + PAYLOAD_SIZE, padding.length);

		// write boot block to disk
		mBlockDevice.writeBlock(0, blockData, 0, mBlockDevice.getBlockSize(), 0L);

		Log.dec();
	}


	private void openBootBlock(final AccessCredentials aCredentials) throws InvalidKeyException, IOException
	{
		Log.i("open boot block");
		Log.inc();

		// read boot block from disk
		byte [] blockData = new byte[mBlockDevice.getBlockSize()];

		try
		{
			mBlockDevice.readBlock(0, blockData, 0, mBlockDevice.getBlockSize(), 0L);
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
			aCredentials.setKeyGeneratorFunction(keyGenerator);

			// create a user key using the key generator
			byte[] userKeyPool = aCredentials.generateKeyPool(salt, ITERATION_COUNT, KEY_POOL_SIZE);

			// decode boot block using all available ciphers
			for (EncryptionFunction ciphers : EncryptionFunction.values())
			{
				byte[] payloadCopy = payload.clone();

				// decrypt payload using the user key
				CipherImplementation cipher = new CipherImplementation(ciphers, userKeyPool, 0, PAYLOAD_SIZE);
				cipher.decrypt(0, payloadCopy, 0, PAYLOAD_SIZE, 0L);

				// read header
				int signature = getInt(payloadCopy, 0);
				int expectedChecksum = getInt(payloadCopy, 4);

				if (signature == SIGNATURE)
				{
					// verify checksum of boot block
					int actualChecksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(payloadCopy, HEADER_SIZE, PAYLOAD_SIZE - HEADER_SIZE, CHECKSUM_SEED);

					if (expectedChecksum == actualChecksum)
					{
						// create the cipher used to encrypt data blocks
						mCipher = new CipherImplementation(ciphers, payloadCopy, HEADER_SIZE, mBlockDevice.getBlockSize());

						Log.dec();

						return;
					}
				}
			}
		}

		Log.w("incorrect password or not a secure BlockDevice");
		Log.dec();

		throw new InvalidKeyException("Incorrect password or not a secure BlockDevice");
	}


	private static final class CipherImplementation
	{
		private transient final byte [][] mIV = new byte[3][IV_SIZE];
		private transient final BlockCipher[] mCiphers;
		private transient final BlockCipher mTweakCipher;
		private transient final CBCCipherMode mCipher;
		private transient final int mUnitSize;


		public CipherImplementation(final EncryptionFunction aCiphers, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitSize)
		{
			mCipher = new CBCCipherMode();
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
				mCipher.encrypt(aBuffer, aOffset, aLength, mCiphers[i], mIV[i], aBlockIndex, aBlockKey, mTweakCipher, mUnitSize);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aBlockKey)
		{
			for (int i = mCiphers.length; --i >= 0; )
			{
				mCipher.decrypt(aBuffer, aOffset, aLength, mCiphers[i], mIV[i], aBlockIndex, aBlockKey, mTweakCipher, mUnitSize);
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
}