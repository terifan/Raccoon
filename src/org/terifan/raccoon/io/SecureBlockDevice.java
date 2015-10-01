package org.terifan.raccoon.io;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import org.terifan.raccoon.security.InvalidKeyException;
import org.terifan.raccoon.security.AES;
import org.terifan.raccoon.security.Cipher;
import org.terifan.raccoon.security.Elephant;
import org.terifan.raccoon.security.MurmurHash3;
import org.terifan.raccoon.security.SecretKey;
import org.terifan.raccoon.security.Serpent;
import org.terifan.raccoon.security.Twofish;
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
//          32 tweak seed
//          96 ciper keys (3 x 32)
//          48 cipher iv (3 x 16)
//      40 payload padding
//   n padding (random, plaintext)

/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device. The block at index 0
 * contain a boot block which store the secret encryption key used to encrypt all other blocks. All read and write operations
 * offset the index to ensure the boot block can never be read/written.
 */
public class SecureBlockDevice implements IPhysicalBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 1;
	private final static int SALT_SIZE = 256;
	private final static int IV_SIZE_INTS = 4;
	private final static int TWEAK_SIZE_INTS = 8;
	private final static int KEY_SIZE_BYTES = 32;
	private final static int HEADER_SIZE = 8;
	private final static int KEY_POOL_SIZE = KEY_SIZE_BYTES + 4 * TWEAK_SIZE_INTS + 3 * KEY_SIZE_BYTES + 3 * 4 * IV_SIZE_INTS;
	private final static int ITERATION_COUNT = 10_000;
	private final static int PAYLOAD_SIZE = 256; // HEADER_SIZE + KEY_POOL_SIZE
	private final static int SIGNATURE = 0xf46a290c;
	private final static int CHECKSUM_SEED = 0x2fc8d359;

	private IPhysicalBlockDevice mBlockDevice;
	private CipherImplementation mCipher;


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
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, final long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		aBlockIndex += RESERVED_BLOCKS;

		Log.v("write block " + aBlockIndex + " +" + aBufferLength/mBlockDevice.getBlockSize());
		Log.inc();

		byte[] workBuffer = aBuffer.clone();

		mCipher.encrypt(aBlockIndex, workBuffer, aBufferOffset, aBufferLength, aBlockKey);

		mBlockDevice.writeBlock(aBlockIndex, workBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		Log.dec();
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, final long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset: " + aBlockIndex);
		}

		aBlockIndex += RESERVED_BLOCKS;

		Log.v("read block " + aBlockIndex + " +" + aBufferLength / mBlockDevice.getBlockSize());
		Log.inc();

		mBlockDevice.readBlock(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L); // block key is used by this blockdevice and not passed to lower levels

		mCipher.decrypt(aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

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
		int checksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(payload, HEADER_SIZE, payload.length - HEADER_SIZE, CHECKSUM_SEED);

		// update header
		putInt(payload, 0, SIGNATURE);
		putInt(payload, 4, checksum);

		// create the cipher used to encrypt data blocks
		mCipher = new CipherImplementation(aCredentials.getEncryptionFunction(), payload, HEADER_SIZE, mBlockDevice.getBlockSize());

		// create user key
		byte[] userKeyPool = aCredentials.generateKeyPool(salt, ITERATION_COUNT, KEY_POOL_SIZE);

		// encrypt payload
		CipherImplementation cipher = new CipherImplementation(aCredentials.getEncryptionFunction(), userKeyPool, 0, payload.length);
		cipher.encrypt(0, payload, 0, payload.length, 0L);

		// assemble output buffer
		byte[] blockData = new byte[mBlockDevice.getBlockSize()];
		System.arraycopy(salt, 0, blockData, 0, salt.length);
		System.arraycopy(payload, 0, blockData, salt.length, payload.length);
		System.arraycopy(padding, 0, blockData, salt.length + payload.length, padding.length);

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
		mBlockDevice.readBlock(0, blockData, 0, mBlockDevice.getBlockSize(), 0L);

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
				CipherImplementation cipher = new CipherImplementation(ciphers, userKeyPool, 0, payloadCopy.length);
				cipher.decrypt(0, payloadCopy, 0, payloadCopy.length, 0L);

				// read header
				int signature = getInt(payloadCopy, 0);
				int expectedChecksum = getInt(payloadCopy, 4);

				if (signature == SIGNATURE)
				{
					// verify checksum of boot block
					int actualChecksum = MurmurHash3.hash_x86_32(salt, CHECKSUM_SEED) ^ MurmurHash3.hash_x86_32(payloadCopy, HEADER_SIZE, payload.length - HEADER_SIZE, CHECKSUM_SEED);

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
		private transient final int [] mTweakKey = new int[8];
		private transient final int [][] mIV = new int[3][4];
		private transient final Cipher[] mCiphers;
		private transient final Cipher mTweakCipher;
		private transient final Elephant mElephant;
		private transient final int mUnitLength;


		public CipherImplementation(final EncryptionFunction aCiphers, final byte[] aKeyPool, final int aKeyPoolOffset, final int aUnitLength)
		{
			switch (aCiphers)
			{
				case AES:
					mCiphers = new Cipher[]{new AES()};
					mTweakCipher = new AES();
					break;
				case Twofish:
					mCiphers = new Cipher[]{new Twofish()};
					mTweakCipher = new Twofish();
					break;
				case Serpent:
					mCiphers = new Cipher[]{new Serpent()};
					mTweakCipher = new Serpent();
					break;
				case AESTwofish:
					mCiphers = new Cipher[]{new AES(), new Twofish()};
					mTweakCipher = new AES();
					break;
				case TwofishSerpent:
					mCiphers = new Cipher[]{new Twofish(), new Serpent()};
					mTweakCipher = new Twofish();
					break;
				case SerpentAES:
					mCiphers = new Cipher[]{new Serpent(), new AES()};
					mTweakCipher = new Serpent();
					break;
				case AESTwofishSerpent:
					mCiphers = new Cipher[]{new AES(), new Twofish(), new Serpent()};
					mTweakCipher = new AES();
					break;
				case TwofishAESSerpent:
					mCiphers = new Cipher[]{new Twofish(), new AES(), new Serpent()};
					mTweakCipher = new Twofish();
					break;
				case SerpentTwofishAES:
					mCiphers = new Cipher[]{new Serpent(), new Twofish(), new AES()};
					mTweakCipher = new Serpent();
					break;
				default:
					throw new IllegalStateException();
			}

			int offset = aKeyPoolOffset;

			mTweakCipher.engineInit(new SecretKey(getBytes(aKeyPool, offset, KEY_SIZE_BYTES)));
			offset += KEY_SIZE_BYTES;

			for (int i = 0; i < TWEAK_SIZE_INTS; i++)
			{
				mTweakKey[i] = getInt(aKeyPool, offset);
				offset += 4;
			}

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
				for (int i = 0; i < IV_SIZE_INTS; i++)
				{
					mIV[j][i] = getInt(aKeyPool, offset);
					offset += 4;
				}
			}

			if (offset != aKeyPoolOffset + KEY_POOL_SIZE)
			{
				throw new IllegalArgumentException("Bad offset: " + offset);
			}

			mUnitLength = aUnitLength;
			mElephant = new Elephant(mUnitLength);
		}


		public void encrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aBlockKey)
		{
			for (int i = 0; i < mCiphers.length; i++)
			{
				mElephant.encrypt(aBuffer, aBuffer, aOffset, aLength, aBlockIndex, mIV[i], mCiphers[i], mTweakCipher, mTweakKey, aBlockKey);
			}
		}


		public void decrypt(final long aBlockIndex, final byte[] aBuffer, final int aOffset, final int aLength, final long aBlockKey)
		{
			for (int i = mCiphers.length; --i >= 0; )
			{
				mElephant.decrypt(aBuffer, aBuffer, aOffset, aLength, aBlockIndex, mIV[i], mCiphers[i], mTweakCipher, mTweakKey, aBlockKey);
			}
		}


		private void reset()
		{
			if (mTweakCipher != null)
			{
				for (Cipher cipher : mCiphers)
				{
					cipher.engineReset();
				}

				mTweakCipher.engineReset();

				mElephant.reset();

				fill(mIV[0], 0);
				fill(mIV[1], 0);
				fill(mIV[2], 0);
				fill(mTweakKey, 0);
				fill(mCiphers, null);
			}
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
