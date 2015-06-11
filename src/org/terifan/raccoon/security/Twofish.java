package org.terifan.raccoon.security;

import java.util.Arrays;


/**
 * Twofish is a balanced 128-bit Feistel symmetric cipher with variable key size.<p>
 *
 * In each round, a 64-bit S-box value is computed from 64 bits of the block,
 * and this value is xored into the other half of the block. The two half-blocks
 * are then exchanged, and the next round begins. Before the first round, all
 * input bits are xored with key-dependent "whitening" subkeys, and after the
 * final round the output bits are xored with other key-dependent whitening
 * subkeys; these subkeys are not used anywhere else in the algorithm.
 *
 * @author
 *    Raif S. Naffah
 */
public final class Twofish extends Cipher
{
	private final static int [] FIXEDPERM0 =
	{
		0xA9,0x67,0xB3,0xE8,0x04,0xFD,0xA3,0x76,0x9A,0x92,0x80,0x78,0xE4,0xDD,0xD1,0x38,
		0x0D,0xC6,0x35,0x98,0x18,0xF7,0xEC,0x6C,0x43,0x75,0x37,0x26,0xFA,0x13,0x94,0x48,
		0xF2,0xD0,0x8B,0x30,0x84,0x54,0xDF,0x23,0x19,0x5B,0x3D,0x59,0xF3,0xAE,0xA2,0x82,
		0x63,0x01,0x83,0x2E,0xD9,0x51,0x9B,0x7C,0xA6,0xEB,0xA5,0xBE,0x16,0x0C,0xE3,0x61,
		0xC0,0x8C,0x3A,0xF5,0x73,0x2C,0x25,0x0B,0xBB,0x4E,0x89,0x6B,0x53,0x6A,0xB4,0xF1,
		0xE1,0xE6,0xBD,0x45,0xE2,0xF4,0xB6,0x66,0xCC,0x95,0x03,0x56,0xD4,0x1C,0x1E,0xD7,
		0xFB,0xC3,0x8E,0xB5,0xE9,0xCF,0xBF,0xBA,0xEA,0x77,0x39,0xAF,0x33,0xC9,0x62,0x71,
		0x81,0x79,0x09,0xAD,0x24,0xCD,0xF9,0xD8,0xE5,0xC5,0xB9,0x4D,0x44,0x08,0x86,0xE7,
		0xA1,0x1D,0xAA,0xED,0x06,0x70,0xB2,0xD2,0x41,0x7B,0xA0,0x11,0x31,0xC2,0x27,0x90,
		0x20,0xF6,0x60,0xFF,0x96,0x5C,0xB1,0xAB,0x9E,0x9C,0x52,0x1B,0x5F,0x93,0x0A,0xEF,
		0x91,0x85,0x49,0xEE,0x2D,0x4F,0x8F,0x3B,0x47,0x87,0x6D,0x46,0xD6,0x3E,0x69,0x64,
		0x2A,0xCE,0xCB,0x2F,0xFC,0x97,0x05,0x7A,0xAC,0x7F,0xD5,0x1A,0x4B,0x0E,0xA7,0x5A,
		0x28,0x14,0x3F,0x29,0x88,0x3C,0x4C,0x02,0xB8,0xDA,0xB0,0x17,0x55,0x1F,0x8A,0x7D,
		0x57,0xC7,0x8D,0x74,0xB7,0xC4,0x9F,0x72,0x7E,0x15,0x22,0x12,0x58,0x07,0x99,0x34,
		0x6E,0x50,0xDE,0x68,0x65,0xBC,0xDB,0xF8,0xC8,0xA8,0x2B,0x40,0xDC,0xFE,0x32,0xA4,
		0xCA,0x10,0x21,0xF0,0xD3,0x5D,0x0F,0x00,0x6F,0x9D,0x36,0x42,0x4A,0x5E,0xC1,0xE0
	};
	private final static int [] FIXEDPERM1 =
	{
		0x75,0xF3,0xC6,0xF4,0xDB,0x7B,0xFB,0xC8,0x4A,0xD3,0xE6,0x6B,0x45,0x7D,0xE8,0x4B,
		0xD6,0x32,0xD8,0xFD,0x37,0x71,0xF1,0xE1,0x30,0x0F,0xF8,0x1B,0x87,0xFA,0x06,0x3F,
		0x5E,0xBA,0xAE,0x5B,0x8A,0x00,0xBC,0x9D,0x6D,0xC1,0xB1,0x0E,0x80,0x5D,0xD2,0xD5,
		0xA0,0x84,0x07,0x14,0xB5,0x90,0x2C,0xA3,0xB2,0x73,0x4C,0x54,0x92,0x74,0x36,0x51,
		0x38,0xB0,0xBD,0x5A,0xFC,0x60,0x62,0x96,0x6C,0x42,0xF7,0x10,0x7C,0x28,0x27,0x8C,
		0x13,0x95,0x9C,0xC7,0x24,0x46,0x3B,0x70,0xCA,0xE3,0x85,0xCB,0x11,0xD0,0x93,0xB8,
		0xA6,0x83,0x20,0xFF,0x9F,0x77,0xC3,0xCC,0x03,0x6F,0x08,0xBF,0x40,0xE7,0x2B,0xE2,
		0x79,0x0C,0xAA,0x82,0x41,0x3A,0xEA,0xB9,0xE4,0x9A,0xA4,0x97,0x7E,0xDA,0x7A,0x17,
		0x66,0x94,0xA1,0x1D,0x3D,0xF0,0xDE,0xB3,0x0B,0x72,0xA7,0x1C,0xEF,0xD1,0x53,0x3E,
		0x8F,0x33,0x26,0x5F,0xEC,0x76,0x2A,0x49,0x81,0x88,0xEE,0x21,0xC4,0x1A,0xEB,0xD9,
		0xC5,0x39,0x99,0xCD,0xAD,0x31,0x8B,0x01,0x18,0x23,0xDD,0x1F,0x4E,0x2D,0xF9,0x48,
		0x4F,0xF2,0x65,0x8E,0x78,0x5C,0x58,0x19,0x8D,0xE5,0x98,0x57,0x67,0x7F,0x05,0x64,
		0xAF,0x63,0xB6,0xFE,0xF5,0xB7,0x3C,0xA5,0xCE,0xE9,0x68,0x44,0xE0,0x4D,0x43,0x69,
		0x29,0x2E,0xAC,0x15,0x59,0xA8,0x0A,0x9E,0x6E,0x47,0xDF,0x34,0x35,0x6A,0xCF,0xDC,
		0x22,0xC9,0xC0,0x9B,0x89,0xD4,0xED,0xAB,0x12,0xA2,0x0D,0x52,0xBB,0x02,0x2F,0xA9,
		0xD7,0x61,0x1E,0xB4,0x50,0x04,0xF6,0xC2,0x16,0x25,0x86,0x56,0x55,0x09,0xBE,0x91
	};

	private final static int [][] MDS = new int[4][256];

	static
	{
		// precompute the MDS matrix
		int[] m1 = new int[2];
		int[] mX = new int[2];
		int[] mY = new int[2];

		int i, j;
		for (i = 0; i < 256; i++)
		{
			j = FIXEDPERM0[i] & 0xFF; // compute all the matrix elements
			m1[0] = j;
			mX[0] = Mx_X(j) & 0xFF;
			mY[0] = Mx_Y(j) & 0xFF;

			j = FIXEDPERM1[i] & 0xFF;
			m1[1] = j;
			mX[1] = Mx_X(j) & 0xFF;
			mY[1] = Mx_Y(j) & 0xFF;

			// fill matrix w/ above elements
			MDS[0][i] = m1[1] <<  0 | mX[1] <<  8 | mY[1] << 16 | mY[1] << 24;
			MDS[1][i] = mY[0] <<  0 | mY[0] <<  8 | mX[0] << 16 | m1[0] << 24;
			MDS[2][i] = mX[1] <<  0 | mY[1] <<  8 | m1[1] << 16 | mY[1] << 24;
			MDS[3][i] = mX[0] <<  0 | m1[0] <<  8 | mY[0] << 16 | mX[0] << 24;
		}
	}

	private static int LFSR1(int x)
	{
		return (x >> 1) ^ ((x & 0x01) != 0 ? 0x169/2 : 0);
	}

	private static int LFSR2(int x)
	{
		return (x >> 2) ^ ((x & 0x02) != 0 ? 0x169/2 : 0) ^ ((x & 0x01) != 0 ? 0x169/4 : 0);
	}

	private static int Mx_X(int x)
	{
		return x ^ LFSR2(x);
	}

	private static int Mx_Y(int x)
	{
		return x ^ LFSR1(x) ^ LFSR2(x);
	}


	private transient int [] mSBox;
	private transient int [] mSubKeys;
	private transient int mKeySize;
	private transient int mSubKey0, mSubKey1, mSubKey2, mSubKey3, mSubKey4, mSubKey5, mSubKey6, mSubKey7;



	public Twofish()
	{
		super();
	}


	public Twofish(SecretKey aSecretKey)
	{
		super(aSecretKey);
	}


	@Override
	public void engineInit(SecretKey aKey)
	{
		byte [] k = aKey.bytes();

		if (!(k.length == 8 || k.length == 16 || k.length == 24 || k.length == 32))
		{
			 throw new IllegalArgumentException("Incorrect key length (8, 16, 24, 32 supported): " + k.length);
		}

		int length = k.length;
		mKeySize = k.length;

		int k64Cnt = length / 8;
		int subkeyCnt = 8 + 2*16;
		int [] k32e = new int[4]; // even 32-bit entities
		int [] k32o = new int[4]; // odd 32-bit entities
		int [] sboxKey = new int[4];

		// split user key material into even and odd 32-bit entities and
		// compute S-box keys using (12, 8) Reed-Solomon code over GF(256)
		int i, j, offset = 0;
		for (i = 0, j = k64Cnt-1; i < 4 && offset < length; i++, j--)
		{
			k32e[i] = (k[offset++] & 0xFF) | (k[offset++] & 0xFF) <<  8 | (k[offset++] & 0xFF) << 16 | (k[offset++] & 0xFF) << 24;
			k32o[i] = (k[offset++] & 0xFF) | (k[offset++] & 0xFF) <<  8 | (k[offset++] & 0xFF) << 16 | (k[offset++] & 0xFF) << 24;
			sboxKey[j] = RS_MDS_Encode(k32e[i], k32o[i]); // reverse order
		}

		// compute the round decryption subkeys for PHT. these same subkeys
		// will be used in encryption but will be applied in reverse order.
		int q, A, B;
		mSubKeys = new int[subkeyCnt];
		for (i = q = 0; i < subkeyCnt/2; i++, q += 0x02020202)
		{
			A = F32(k64Cnt, q, k32e); // A uses even key entities
			B = F32(k64Cnt, q+0x01010101, k32o); // B uses odd  key entities
			B = B << 8 | B >>> 24;
			A += B;
			mSubKeys[i<<1] = A;					// combine with a PHT
			A += B;
			mSubKeys[(i<<1) + 1] = A << 9 | A >>> (32-9);
		}

		mSubKey0 = mSubKeys[0];
		mSubKey1 = mSubKeys[1];
		mSubKey2 = mSubKeys[2];
		mSubKey3 = mSubKeys[3];
		mSubKey4 = mSubKeys[4];
		mSubKey5 = mSubKeys[5];
		mSubKey6 = mSubKeys[6];
		mSubKey7 = mSubKeys[7];

		// fully expand the table for speed
		int k0 = sboxKey[0];
		int k1 = sboxKey[1];
		int k2 = sboxKey[2];
		int k3 = sboxKey[3];
		int b0, b1, b2, b3;
		mSBox = new int[4 * 256];

		for (i = 0; i < 256; i++)
		{
			b0 = b1 = b2 = b3 = i;
			switch (k64Cnt & 3)
			{
				case 1:
					mSBox[      2*i  ] = MDS[0][(FIXEDPERM0[b0] & 0xFF) ^ b0(k0)];
					mSBox[      2*i+1] = MDS[1][(FIXEDPERM0[b1] & 0xFF) ^ b1(k0)];
					mSBox[0x200+2*i  ] = MDS[2][(FIXEDPERM1[b2] & 0xFF) ^ b2(k0)];
					mSBox[0x200+2*i+1] = MDS[3][(FIXEDPERM1[b3] & 0xFF) ^ b3(k0)];
					break;
				case 0: // same as 4
					b0 = (FIXEDPERM1[b0] & 0xFF) ^ b0(k3);
					b1 = (FIXEDPERM0[b1] & 0xFF) ^ b1(k3);
					b2 = (FIXEDPERM0[b2] & 0xFF) ^ b2(k3);
					b3 = (FIXEDPERM1[b3] & 0xFF) ^ b3(k3);
				case 3:
					b0 = (FIXEDPERM1[b0] & 0xFF) ^ b0(k2);
					b1 = (FIXEDPERM1[b1] & 0xFF) ^ b1(k2);
					b2 = (FIXEDPERM0[b2] & 0xFF) ^ b2(k2);
					b3 = (FIXEDPERM0[b3] & 0xFF) ^ b3(k2);
				case 2: // 128-bit keys
					mSBox[      2*i  ] = MDS[0][(FIXEDPERM0[(FIXEDPERM0[b0] & 0xFF) ^ b0(k1)] & 0xFF) ^ b0(k0)];
					mSBox[      2*i+1] = MDS[1][(FIXEDPERM0[(FIXEDPERM1[b1] & 0xFF) ^ b1(k1)] & 0xFF) ^ b1(k0)];
					mSBox[0x200+2*i  ] = MDS[2][(FIXEDPERM1[(FIXEDPERM0[b2] & 0xFF) ^ b2(k1)] & 0xFF) ^ b2(k0)];
					mSBox[0x200+2*i+1] = MDS[3][(FIXEDPERM1[(FIXEDPERM1[b3] & 0xFF) ^ b3(k1)] & 0xFF) ^ b3(k0)];
			}
		}
	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.<p>
	 *
	 * Note: It is possible to use the same buffer for input and output.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
//	@Override
//	public void engineEncryptBlock(byte [] in, int inOffset, byte [] out, int outOffset)
//	{
//		int [] sbox = mSBox;
//		int [] skey = mSubKeys;
//
//		int x0 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey0;
//		int x1 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey1;
//		int x2 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey2;
//		int x3 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset  ] << 24)) ^ mSubKey3;
//
//		int k = 8;
//
//		// unrolled for-loop, 8 iterations
//
//		// round 0-1
//		int t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		int t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 2-3
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 4-5
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 6-7
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 8-9
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 10-11
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 12-13
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//		// round 14-15
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x2 ^= t0 + t1 + skey[k++];
//		x2  = x2 >>> 1 | x2 << 31;
//		x3  = x3 << 1 | x3 >>> 31;
//		x3 ^= t0 + 2*t1 + skey[k++];
//
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x0 ^= t0 + t1 + skey[k++];
//		x0  = x0 >>> 1 | x0 << 31;
//		x1  = x1 << 1 | x1 >>> 31;
//		x1 ^= t0 + 2*t1 + skey[k++];
//
//
//		x2 ^= mSubKey4;
//		x3 ^= mSubKey5;
//		x0 ^= mSubKey6;
//		x1 ^= mSubKey7;
//
//		out[outOffset++] = (byte)x2; out[outOffset++] = (byte)(x2 >> 8); out[outOffset++] = (byte)(x2 >> 16); out[outOffset++] = (byte)(x2 >>> 24);
//		out[outOffset++] = (byte)x3; out[outOffset++] = (byte)(x3 >> 8); out[outOffset++] = (byte)(x3 >> 16); out[outOffset++] = (byte)(x3 >>> 24);
//		out[outOffset++] = (byte)x0; out[outOffset++] = (byte)(x0 >> 8); out[outOffset++] = (byte)(x0 >> 16); out[outOffset++] = (byte)(x0 >>> 24);
//		out[outOffset++] = (byte)x1; out[outOffset++] = (byte)(x1 >> 8); out[outOffset++] = (byte)(x1 >> 16); out[outOffset++] = (byte)(x1 >>> 24);
//	}
//
//
//	/**
//	 * Decrypts a single block of ciphertext in ECB-mode.<p>
//	 *
//	 * Note: It is possible to use the same buffer for input and output.
//	 *
//	 * @param in
//	 *    A buffer containing the ciphertext to be decrypted.
//	 * @param inOffset
//	 *    Index in the in buffer where ciphertext should be read.
//	 * @param out
//	 *    A buffer where plaintext is written.
//	 * @param outOffset
//	 *    Index in the out buffer where plaintext should be written.
//	 */
//	@Override
//	public void engineDecryptBlock(byte [] in, int inOffset, byte [] out, int outOffset)
//	{
//		int [] sbox = mSBox;
//		int [] skey = mSubKeys;
//
//		int x2 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey4;
//		int x3 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey5;
//		int x0 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset++] << 24)) ^ mSubKey6;
//		int x1 = ((255 & in[inOffset++]) + ((255 & in[inOffset++]) << 8) + ((255 & in[inOffset++]) << 16) + (in[inOffset  ] << 24)) ^ mSubKey7;
//
//		int k = 39;
//
//		// unrolled for-loop, 8 iterations
//
//		// round 0-1
//		int t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		int t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 2-3
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 4-5
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 6-7
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 8-9
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 10-11
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 12-13
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//		// round 14-15
//		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
//		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
//		x1 ^= t0 + 2*t1 + skey[k--];
//		x1  = x1 >>> 1 | x1 << 31;
//		x0  = x0 << 1 | x0 >>> 31;
//		x0 ^= t0 + t1 + skey[k--];
//
//		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
//		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
//		x3 ^= t0 + 2*t1 + skey[k--];
//		x3  = x3 >>> 1 | x3 << 31;
//		x2  = x2 << 1 | x2 >>> 31;
//		x2 ^= t0 + t1 + skey[k--];
//
//
//		x0 ^= mSubKey0;
//		x1 ^= mSubKey1;
//		x2 ^= mSubKey2;
//		x3 ^= mSubKey3;
//
//		out[outOffset++] = (byte)x0; out[outOffset++] = (byte)(x0 >> 8); out[outOffset++] = (byte)(x0 >> 16); out[outOffset++] = (byte)(x0 >>> 24);
//		out[outOffset++] = (byte)x1; out[outOffset++] = (byte)(x1 >> 8); out[outOffset++] = (byte)(x1 >> 16); out[outOffset++] = (byte)(x1 >>> 24);
//		out[outOffset++] = (byte)x2; out[outOffset++] = (byte)(x2 >> 8); out[outOffset++] = (byte)(x2 >> 16); out[outOffset++] = (byte)(x2 >>> 24);
//		out[outOffset++] = (byte)x3; out[outOffset++] = (byte)(x3 >> 8); out[outOffset++] = (byte)(x3 >> 16); out[outOffset++] = (byte)(x3 >>> 24);
//	}


	/**
	 * Encrypts a single block of plaintext in ECB-mode.<p>
	 *
	 * Note: It is possible to use the same buffer for input and output.
	 *
	 * @param in
	 *    A buffer containing the plaintext to be encrypted.
	 * @param inOffset
	 *    Index in the in buffer where plaintext should be read.
	 * @param out
	 *    A buffer where ciphertext is written.
	 * @param outOffset
	 *    Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(int [] in, int inOffset, int [] out, int outOffset)
	{
		int [] sbox = mSBox;
		int [] skey = mSubKeys;

		int x0 = reverseBytes(in[inOffset++]) ^ mSubKey0;
		int x1 = reverseBytes(in[inOffset++]) ^ mSubKey1;
		int x2 = reverseBytes(in[inOffset++]) ^ mSubKey2;
		int x3 = reverseBytes(in[inOffset  ]) ^ mSubKey3;

		int k = 8;

		// unrolled for-loop, 8 iterations

		// round 0-1
		int t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		int t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 2-3
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 4-5
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 6-7
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 8-9
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 10-11
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 12-13
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];

		// round 14-15
		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x2 ^= t0 + t1 + skey[k++];
		x2  = x2 >>> 1 | x2 << 31;
		x3  = x3 << 1 | x3 >>> 31;
		x3 ^= t0 + 2*t1 + skey[k++];

		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x0 ^= t0 + t1 + skey[k++];
		x0  = x0 >>> 1 | x0 << 31;
		x1  = x1 << 1 | x1 >>> 31;
		x1 ^= t0 + 2*t1 + skey[k++];


		out[outOffset++] = reverseBytes(x2 ^ mSubKey4);
		out[outOffset++] = reverseBytes(x3 ^ mSubKey5);
		out[outOffset++] = reverseBytes(x0 ^ mSubKey6);
		out[outOffset  ] = reverseBytes(x1 ^ mSubKey7);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.<p>
	 *
	 * Note: It is possible to use the same buffer for input and output.
	 *
	 * @param in
	 *    A buffer containing the ciphertext to be decrypted.
	 * @param inOffset
	 *    Index in the in buffer where ciphertext should be read.
	 * @param out
	 *    A buffer where plaintext is written.
	 * @param outOffset
	 *    Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(int [] in, int inOffset, int [] out, int outOffset)
	{
		int [] sbox = mSBox;
		int [] skey = mSubKeys;

		int x2 = reverseBytes(in[inOffset++]) ^ mSubKey4;
		int x3 = reverseBytes(in[inOffset++]) ^ mSubKey5;
		int x0 = reverseBytes(in[inOffset++]) ^ mSubKey6;
		int x1 = reverseBytes(in[inOffset  ]) ^ mSubKey7;

		int k = 39;

		// unrolled for-loop, 8 iterations

		// round 0-1
		int t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		int t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 2-3
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 4-5
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 6-7
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 8-9
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 10-11
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 12-13
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];

		// round 14-15
		t0 = sbox[(510 & (x2 <<   1))] ^ sbox[1 + (510 & (x2 >> 7))] ^ sbox[512 + (510 & (x2 >> 15))] ^ sbox[513 + (510 & (x2 >>> 23))];
		t1 = sbox[(510 & (x3 >>> 23))] ^ sbox[1 + (510 & (x3 << 1))] ^ sbox[512 + (510 & (x3 >>  7))] ^ sbox[513 + (510 & (x3 >>  15))];
		x1 ^= t0 + 2*t1 + skey[k--];
		x1  = x1 >>> 1 | x1 << 31;
		x0  = x0 << 1 | x0 >>> 31;
		x0 ^= t0 + t1 + skey[k--];

		t0 = sbox[(510 & (x0 <<   1))] ^ sbox[1 + (510 & (x0 >> 7))] ^ sbox[512 + (510 & (x0 >> 15))] ^ sbox[513 + (510 & (x0 >>> 23))];
		t1 = sbox[(510 & (x1 >>> 23))] ^ sbox[1 + (510 & (x1 << 1))] ^ sbox[512 + (510 & (x1 >>  7))] ^ sbox[513 + (510 & (x1 >>  15))];
		x3 ^= t0 + 2*t1 + skey[k--];
		x3  = x3 >>> 1 | x3 << 31;
		x2  = x2 << 1 | x2 >>> 31;
		x2 ^= t0 + t1 + skey[k--];


		out[outOffset++] = reverseBytes(x0 ^ mSubKey0);
		out[outOffset++] = reverseBytes(x1 ^ mSubKey1);
		out[outOffset++] = reverseBytes(x2 ^ mSubKey2);
		out[outOffset  ] = reverseBytes(x3 ^ mSubKey3);
	}


	private static int b0(int x) { return (x       ) & 0xFF; }
	private static int b1(int x) { return (x >>>  8) & 0xFF; }
	private static int b2(int x) { return (x >>> 16) & 0xFF; }
	private static int b3(int x) { return (x >>> 24) & 0xFF; }


	/**
	 * Use (12, 8) Reed-Solomon code over GF(256) to produce a key S-box
	 * 32-bit entity from two key material 32-bit entities.
	 *
	 * @param  k0  1st 32-bit entity.
	 * @param  k1  2nd 32-bit entity.
	 * @return  Remainder polynomial generated using RS code
	 */
	private static int RS_MDS_Encode(int k0, int k1)
	{
		int r = k1;

		for (int i = 0; i < 4; i++) // shift 1 byte at a time
		{
			r = RS_rem(r);
		}

		r ^= k0;

		for (int i = 0; i < 4; i++)
		{
			r = RS_rem(r);
		}

		return r;
	}


	/**
	 * Reed-Solomon code parameters: (12, 8) reversible code:<p>
	 *
	 *	g(x) = x**4 + (a + 1/a) x**3 + a x**2 + (a + 1/a) x + 1
	 *
	 * where a = primitive root of field generator 0x14D
	 */
	private static int RS_rem(int x)
	{
		int b  =  (x >>> 24) & 0xFF;
		int g2 = ((b  <<  1) ^ ((b & 0x80) != 0 ? 0x14D : 0)) & 0xFF;
		int g3 =  (b >>>  1) ^ ((b & 0x01) != 0 ? (0x14D >>> 1) : 0) ^ g2 ;
		int result = (x << 8) ^ (g3 << 24) ^ (g2 << 16) ^ (g3 << 8) ^ b;

		return result;
	}


	private static int F32(int k64Cnt, int x, int[] k32)
	{
		int b0 = b0(x);
		int b1 = b1(x);
		int b2 = b2(x);
		int b3 = b3(x);
		int k0 = k32[0];
		int k1 = k32[1];
		int k2 = k32[2];
		int k3 = k32[3];

		int result = 0;
		switch (k64Cnt & 3)
		{
			case 1:
				result = MDS[0][(FIXEDPERM0[b0] & 0xFF) ^ b0(k0)] ^
				         MDS[1][(FIXEDPERM0[b1] & 0xFF) ^ b1(k0)] ^
				         MDS[2][(FIXEDPERM1[b2] & 0xFF) ^ b2(k0)] ^
				         MDS[3][(FIXEDPERM1[b3] & 0xFF) ^ b3(k0)];
				break;
			case 0:  // same as 4
				b0 = (FIXEDPERM1[b0] & 0xFF) ^ b0(k3);
				b1 = (FIXEDPERM0[b1] & 0xFF) ^ b1(k3);
				b2 = (FIXEDPERM0[b2] & 0xFF) ^ b2(k3);
				b3 = (FIXEDPERM1[b3] & 0xFF) ^ b3(k3);
			case 3:
				b0 = (FIXEDPERM1[b0] & 0xFF) ^ b0(k2);
				b1 = (FIXEDPERM1[b1] & 0xFF) ^ b1(k2);
				b2 = (FIXEDPERM0[b2] & 0xFF) ^ b2(k2);
				b3 = (FIXEDPERM0[b3] & 0xFF) ^ b3(k2);
			case 2:	// 128-bit keys (optimize for this case)
				result = MDS[0][(FIXEDPERM0[(FIXEDPERM0[b0] & 0xFF) ^ b0(k1)] & 0xFF) ^ b0(k0)] ^
				         MDS[1][(FIXEDPERM0[(FIXEDPERM1[b1] & 0xFF) ^ b1(k1)] & 0xFF) ^ b1(k0)] ^
				         MDS[2][(FIXEDPERM1[(FIXEDPERM0[b2] & 0xFF) ^ b2(k1)] & 0xFF) ^ b2(k0)] ^
				         MDS[3][(FIXEDPERM1[(FIXEDPERM1[b3] & 0xFF) ^ b3(k1)] & 0xFF) ^ b3(k0)];
				break;
		}

		return result;
	}


	/**
	 * Returns the block size.
	 */
	@Override
	public int engineGetBlockSize()
	{
		return 16;
	}


	/**
	 * Returns the key size.
	 */
	@Override
	public int engineGetKeySize()
	{
		return mKeySize;
	}


	/**
	 * Resets all internal state data. This Cipher object needs to be
	 * reinitialized again before it can be used again.
	 */
	@Override
	public void engineReset()
	{
		mKeySize = 0;
		if (mSBox != null) Arrays.fill(mSBox, -1);
		if (mSBox != null) Arrays.fill(mSBox, 0);
		if (mSubKeys != null) Arrays.fill(mSubKeys, -1);
		if (mSubKeys != null) Arrays.fill(mSubKeys, 0);
		mSubKey0 = mSubKey1 = mSubKey2 = mSubKey3 = 0;
		mSubKey4 = mSubKey5 = mSubKey6 = mSubKey7 = 0;
		mSBox = null;
		mSubKeys = null;
	}


	@Override
	public String toString()
	{
		return "Twofish";
	}


	/**
	 * This method returns a new instance of the Twofish cipher.
	 */
	@Override
	public Cipher newInstance()
	{
		return new Twofish();
	}


    private static int reverseBytes(int i)
    {
        return ((i >>> 24)           ) +
               ((i >>   8) &   0xFF00) +
               ((i <<   8) & 0xFF0000) +
               ((i << 24));
    }
}