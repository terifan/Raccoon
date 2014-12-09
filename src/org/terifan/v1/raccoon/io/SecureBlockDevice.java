package org.terifan.v1.raccoon.io;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;
import org.terifan.v1.security.HMAC;
import org.terifan.v1.security.InvalidKeyException;
import org.terifan.v1.security.SHA512;
import org.terifan.v1.security.Skein512;
import org.terifan.v1.security.Whirlpool;
import org.terifan.v1.security.AES;
import org.terifan.v1.security.Cipher;
import org.terifan.v1.security.Elephant;
import org.terifan.v1.security.PBKDF2;
import org.terifan.v1.security.SecretKey;
import org.terifan.v1.security.Serpent;
import org.terifan.v1.security.Twofish;


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
//      40 block key pool
//           8 block key (5 x 8)
//   n padding (random, plaintext)

/**
 * The SecureBlockDevice encrypt blocks as they are written to the underlying physical block device. The block at index 0
 * contain a boot block which store the secret encryption key used to encrypt all other blocks. All read and write operations
 * offset the index to ensure the boot block can never be written to.
 */
public class SecureBlockDevice implements IPhysicalBlockDevice, AutoCloseable
{
	private final static int RESERVED_BLOCKS = 1;
	private final static int SALT_SIZE = 256;
	private final static int IV_SIZE = 16;
	private final static int TWEAK_SIZE = 32;
	private final static int KEY_SIZE_BYTES = 32;
	private final static int HEADER_SIZE = 8;
	private final static int KEY_POOL_SIZE = 4 * KEY_SIZE_BYTES + 3 * IV_SIZE + TWEAK_SIZE + 5 * 8;
	private final static int ITERATION_COUNT = 10_000;
	private final static int PAYLOAD_SIZE = 256; // HEADER_SIZE + KEY_POOL_SIZE
	private final static int SIGNATURE = 0xf46a290c;

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
	public void writeBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset " + aBlockIndex);
		}

		aBuffer = aBuffer.clone();

		mCipher.encrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);

		mBlockDevice.writeBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L);
	}


	@Override
	public void readBlock(long aBlockIndex, byte[] aBuffer, int aBufferOffset, int aBufferLength, long aBlockKey) throws IOException
	{
		if (aBlockIndex < 0)
		{
			throw new IOException("Illegal offset " + aBlockIndex);
		}

		mBlockDevice.readBlock(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, 0L);

		mCipher.decrypt(RESERVED_BLOCKS + aBlockIndex, aBuffer, aBufferOffset, aBufferLength, aBlockKey);
	}


	@Override
	public int getBlockSize()
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


	private void createBootBlock(AccessCredentials aCredentials) throws IOException
	{
		byte[] salt = new byte[SALT_SIZE];
		byte[] payload = new byte[PAYLOAD_SIZE];
		byte[] padding = new byte[mBlockDevice.getBlockSize() - SALT_SIZE - PAYLOAD_SIZE];

		// create the secret keys
		SecureRandom rnd = new SecureRandom();
		rnd.nextBytes(payload);

		Random rand = new Random();
		rand.nextBytes(padding);
		rand.nextBytes(salt);

		// compute checksum
		CRC32 crc = new CRC32();
		crc.update(salt);
		crc.update(payload, HEADER_SIZE, payload.length - HEADER_SIZE);

		// update header
		putInt(payload, SIGNATURE, 0);
		putInt(payload, (int)crc.getValue(), 4);

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
	}


	private void openBootBlock(AccessCredentials aCredentials) throws InvalidKeyException, IOException
	{
		// read boot block from disk
		byte [] blockData = new byte[mBlockDevice.getBlockSize()];
		mBlockDevice.readBlock(0, blockData, 0, mBlockDevice.getBlockSize(), 0L);

		// extract the salt and payload
		byte[] salt = Arrays.copyOfRange(blockData, 0, SALT_SIZE);
		byte[] payload = Arrays.copyOfRange(blockData, SALT_SIZE, SALT_SIZE + PAYLOAD_SIZE);

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
				int checksum = getInt(payloadCopy, 4);

				if (signature == SIGNATURE)
				{
					// verify checksum of boot block
					CRC32 crc = new CRC32();
					crc.update(salt);
					crc.update(payloadCopy, HEADER_SIZE, payload.length - HEADER_SIZE);

					if (checksum == (int)crc.getValue())
					{
						// create the cipher used to encrypt data blocks
						mCipher = new CipherImplementation(ciphers, payloadCopy, HEADER_SIZE, mBlockDevice.getBlockSize());

						return;
					}
				}
			}
		}

		throw new InvalidKeyException("Incorrect password");
	}


	private static class CipherImplementation
	{
		private transient final long[] mBlockKeyPool = new long[5];
		private transient final int [] mTweakKey = new int[8];
		private transient final int [][] mIV = new int[3][4];
		private transient Cipher[] mCiphers;
		private transient Cipher mTweakCipher;
		private transient Elephant mElephant;
		private transient int mUnitLength;


		public CipherImplementation(EncryptionFunction aCiphers, byte[] aKeyPool, int aOffset, int aUnitLength)
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

			mTweakCipher.init(new SecretKey(aKeyPool, aOffset, KEY_SIZE_BYTES));
			aOffset += KEY_SIZE_BYTES;

			for (int i = 0; i < TWEAK_SIZE / 4; i++)
			{
				mTweakKey[i] = getInt(aKeyPool, aOffset);
				aOffset += 4;
			}

			for (Cipher cipher : mCiphers)
			{
				cipher.init(new SecretKey(aKeyPool, aOffset, KEY_SIZE_BYTES));
				aOffset += KEY_SIZE_BYTES;
			}

			for (int j = 0; j < 3; j++)
			{
				for (int i = 0; i < IV_SIZE / 4; i++)
				{
					mIV[j][i] = getInt(aKeyPool, aOffset);
					aOffset += 4;
				}
			}

			for (int i = 0; i < mBlockKeyPool.length; i++)
			{
				mBlockKeyPool[i] = getLong(aKeyPool, aOffset);
				aOffset += 8;
			}

			mUnitLength = aUnitLength;
			mElephant = new Elephant(mUnitLength);
		}


		public void encrypt(long aBlockIndex, byte[] aBuffer, int aOffset, int aLength, long aBlockKey)
		{
			aBlockKey = mixBlockKey(aBlockIndex, aLength, aBlockKey);

//			Log.out.printf("%8d %8d   %016x\n", aBlockIndex, aLength, aBlockKey);

			for (int i = 0; i < mCiphers.length; i++)
			{
				mElephant.encrypt(aBuffer, aOffset, aLength, aBlockIndex, mCiphers[i], mTweakCipher, mIV[i], mTweakKey, aBlockKey);
			}
		}


		public void decrypt(long aBlockIndex, byte[] aBuffer, int aOffset, int aLength, long aBlockKey)
		{
			aBlockKey = mixBlockKey(aBlockIndex, aLength, aBlockKey);

			for (int i = mCiphers.length; --i >= 0; )
			{
				mElephant.decrypt(aBuffer, aOffset, aLength, aBlockIndex, mCiphers[i], mTweakCipher, mIV[i], mTweakKey, aBlockKey);
			}
		}


		/**
		 * Mix the provided block key with a key from the block key pool.
		 */
		private long mixBlockKey(long aBlockIndex, int aLength, long aBlockKey)
		{
			if (aBlockIndex == 1 || aBlockIndex == 2)
			{
				if (aLength != mUnitLength)
				{
					throw new UnsupportedOperationException("not implemented");
				}

				aBlockKey ^= mBlockKeyPool[(int)aBlockIndex - 1];
			}

			return aBlockKey;
		}


		private void reset()
		{
			if (mTweakCipher != null)
			{
				for (Cipher cipher : mCiphers)
				{
					cipher.reset();
				}

				mTweakCipher.reset();

				mElephant.reset();

				Arrays.fill(mIV[0], 0);
				Arrays.fill(mIV[1], 0);
				Arrays.fill(mIV[2], 0);
				Arrays.fill(mTweakKey, 0);
				Arrays.fill(mCiphers, null);
				mTweakCipher = null;
				mCiphers = null;
				mElephant = null;
			}
		}
	};


	private static int getInt(byte[] aBuffer, int aOffset)
	{
		return ((0xff & aBuffer[aOffset + 0]) << 24)
			 + ((0xff & aBuffer[aOffset + 1]) << 16)
			 + ((0xff & aBuffer[aOffset + 2]) <<  8)
			 + ((0xff & aBuffer[aOffset + 3])      );
	}


	private static void putInt(byte[] aBuffer, int aValue, int aOffset)
	{
		aBuffer[aOffset + 0] = (byte)(aValue >>> 24);
		aBuffer[aOffset + 1] = (byte)(aValue >> 16);
		aBuffer[aOffset + 2] = (byte)(aValue >> 8);
		aBuffer[aOffset + 3] = (byte)(aValue);
	}


	private static long getLong(byte [] aBuffer, int aOffset)
	{
		return (((long)(aBuffer[aOffset + 7]      ) << 56)
			  + ((long)(aBuffer[aOffset + 6] & 255) << 48)
			  + ((long)(aBuffer[aOffset + 5] & 255) << 40)
			  + ((long)(aBuffer[aOffset + 4] & 255) << 32)
			  + ((long)(aBuffer[aOffset + 3] & 255) << 24)
			  + ((      aBuffer[aOffset + 2] & 255) << 16)
			  + ((      aBuffer[aOffset + 1] & 255) <<  8)
			  + ((      aBuffer[aOffset    ] & 255       )));
	}
}
