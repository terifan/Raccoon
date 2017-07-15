package org.terifan.security.cryptography;


/**
 * Serpent is a 128-bit 32-round block cipher with variable key lengths, including 128-, 192- and 256-bit keys conjectured to be at least as
 * secure as three-key triple-DES.<p>
 *
 * Serpent was designed by Ross Anderson, Eli Biham and Lars Knudsen as a candidate algorithm for the NIST AES Quest.<p>
 *
 * References:<ol>
 * <li>Serpent: A New Block Cipher Proposal. This paper was published in the proceedings of the "Fast Software Encryption Workshop No. 5"
 * held in Paris in March 1998. LNCS, Springer Verlag.<p>
 * <li>Reference implementation of the standard Serpent cipher written in C by <a href="http://www.cl.cam.ac.uk/~fms/"> Frank
 * Stajano</a>.</ol><p>
 *
 * <b>Copyright</b> &copy; 1997, 1998
 * <a href="http://www.systemics.com/">Systemics Ltd</a> on behalf of the
 * <a href="http://www.systemics.com/docs/cryptix/">Cryptix Development Team</a>.
 * <br>All rights reserved.<p>
 *
 * @author Raif S. Naffah
 * @author Serpent authors (Ross Anderson, Eli Biham and Lars Knudsen)
 */
public class Serpent implements BlockCipher
{
	private static final int ROUNDS = 32;

	// The fractional part of the golden ratio, (sqrt(5)+1)/2.
	private static final int PHI = 0x9e3779b9;

	private transient int k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13,
		k14, k15, k16, k17, k18, k19, k20, k21, k22, k23, k24, k25, k26,
		k27, k28, k29, k30, k31, k32, k33, k34, k35, k36, k37, k38, k39,
		k40, k41, k42, k43, k44, k45, k46, k47, k48, k49, k50, k51, k52,
		k53, k54, k55, k56, k57, k58, k59, k60, k61, k62, k63, k64, k65,
		k66, k67, k68, k69, k70, k71, k72, k73, k74, k75, k76, k77, k78,
		k79, k80, k81, k82, k83, k84, k85, k86, k87, k88, k89, k90, k91,
		k92, k93, k94, k95, k96, k97, k98, k99, k100, k101, k102, k103,
		k104, k105, k106, k107, k108, k109, k110, k111, k112, k113, k114,
		k115, k116, k117, k118, k119, k120, k121, k122, k123, k124, k125,
		k126, k127, k128, k129, k130, k131;

	public Serpent()
	{
	}


	public Serpent(SecretKey aSecretKey)
	{
		engineInit(aSecretKey);
	}


	@Override
	public void engineInit(SecretKey aKey)
	{
		engineReset();

		byte[] kb = aKey.bytes();

		if (kb == null || (kb.length != 16 && kb.length != 24 && kb.length != 32))
		{
			throw new IllegalArgumentException("Invalid k: " + (kb == null ? "null" : kb.length + " bytes != {16,24,32}"));
		}

		// Here w is our "pre-key".
		int[] w = new int[4 * (ROUNDS + 1)];
		int i, j;
		for (i = 0, j = 0; i < 8 && j < kb.length; i++)
		{
			w[i] = (kb[j++] & 0xff) | (kb[j++] & 0xff) << 8
				| (kb[j++] & 0xff) << 16 | (kb[j++] & 0xff) << 24;
		}
		// Pad key if < 256 bits.
		if (i != 8)
		{
			w[i] = 1;
		}
		// Transform using w_i-8 ... w_i-1
		for (i = 8, j = 0; i < 16; i++)
		{
			int t = w[j] ^ w[i - 5] ^ w[i - 3] ^ w[i - 1] ^ PHI ^ j++;
			w[i] = t << 11 | t >>> 21;
		}
		// Translate by 8.
		for (i = 0; i < 8; i++)
		{
			w[i] = w[i + 8];
		}
		// Transform the rest of the key.
		for (; i < w.length; i++)
		{
			int t = w[i - 8] ^ w[i - 5] ^ w[i - 3] ^ w[i - 1] ^ PHI ^ i;
			w[i] = t << 11 | t >>> 21;
		}
		
		State state = new State();

		// After these s-boxes the pre-key (w, above) will become the
		// session key (key, below).
		state.sbox3(w[0], w[1], w[2], w[3]);
		k0 = state.x0;
		k1 = state.x1;
		k2 = state.x2;
		k3 = state.x3;
		state.sbox2(w[4], w[5], w[6], w[7]);
		k4 = state.x0;
		k5 = state.x1;
		k6 = state.x2;
		k7 = state.x3;
		state.sbox1(w[8], w[9], w[10], w[11]);
		k8 = state.x0;
		k9 = state.x1;
		k10 = state.x2;
		k11 = state.x3;
		state.sbox0(w[12], w[13], w[14], w[15]);
		k12 = state.x0;
		k13 = state.x1;
		k14 = state.x2;
		k15 = state.x3;
		state.sbox7(w[16], w[17], w[18], w[19]);
		k16 = state.x0;
		k17 = state.x1;
		k18 = state.x2;
		k19 = state.x3;
		state.sbox6(w[20], w[21], w[22], w[23]);
		k20 = state.x0;
		k21 = state.x1;
		k22 = state.x2;
		k23 = state.x3;
		state.sbox5(w[24], w[25], w[26], w[27]);
		k24 = state.x0;
		k25 = state.x1;
		k26 = state.x2;
		k27 = state.x3;
		state.sbox4(w[28], w[29], w[30], w[31]);
		k28 = state.x0;
		k29 = state.x1;
		k30 = state.x2;
		k31 = state.x3;
		state.sbox3(w[32], w[33], w[34], w[35]);
		k32 = state.x0;
		k33 = state.x1;
		k34 = state.x2;
		k35 = state.x3;
		state.sbox2(w[36], w[37], w[38], w[39]);
		k36 = state.x0;
		k37 = state.x1;
		k38 = state.x2;
		k39 = state.x3;
		state.sbox1(w[40], w[41], w[42], w[43]);
		k40 = state.x0;
		k41 = state.x1;
		k42 = state.x2;
		k43 = state.x3;
		state.sbox0(w[44], w[45], w[46], w[47]);
		k44 = state.x0;
		k45 = state.x1;
		k46 = state.x2;
		k47 = state.x3;
		state.sbox7(w[48], w[49], w[50], w[51]);
		k48 = state.x0;
		k49 = state.x1;
		k50 = state.x2;
		k51 = state.x3;
		state.sbox6(w[52], w[53], w[54], w[55]);
		k52 = state.x0;
		k53 = state.x1;
		k54 = state.x2;
		k55 = state.x3;
		state.sbox5(w[56], w[57], w[58], w[59]);
		k56 = state.x0;
		k57 = state.x1;
		k58 = state.x2;
		k59 = state.x3;
		state.sbox4(w[60], w[61], w[62], w[63]);
		k60 = state.x0;
		k61 = state.x1;
		k62 = state.x2;
		k63 = state.x3;
		state.sbox3(w[64], w[65], w[66], w[67]);
		k64 = state.x0;
		k65 = state.x1;
		k66 = state.x2;
		k67 = state.x3;
		state.sbox2(w[68], w[69], w[70], w[71]);
		k68 = state.x0;
		k69 = state.x1;
		k70 = state.x2;
		k71 = state.x3;
		state.sbox1(w[72], w[73], w[74], w[75]);
		k72 = state.x0;
		k73 = state.x1;
		k74 = state.x2;
		k75 = state.x3;
		state.sbox0(w[76], w[77], w[78], w[79]);
		k76 = state.x0;
		k77 = state.x1;
		k78 = state.x2;
		k79 = state.x3;
		state.sbox7(w[80], w[81], w[82], w[83]);
		k80 = state.x0;
		k81 = state.x1;
		k82 = state.x2;
		k83 = state.x3;
		state.sbox6(w[84], w[85], w[86], w[87]);
		k84 = state.x0;
		k85 = state.x1;
		k86 = state.x2;
		k87 = state.x3;
		state.sbox5(w[88], w[89], w[90], w[91]);
		k88 = state.x0;
		k89 = state.x1;
		k90 = state.x2;
		k91 = state.x3;
		state.sbox4(w[92], w[93], w[94], w[95]);
		k92 = state.x0;
		k93 = state.x1;
		k94 = state.x2;
		k95 = state.x3;
		state.sbox3(w[96], w[97], w[98], w[99]);
		k96 = state.x0;
		k97 = state.x1;
		k98 = state.x2;
		k99 = state.x3;
		state.sbox2(w[100], w[101], w[102], w[103]);
		k100 = state.x0;
		k101 = state.x1;
		k102 = state.x2;
		k103 = state.x3;
		state.sbox1(w[104], w[105], w[106], w[107]);
		k104 = state.x0;
		k105 = state.x1;
		k106 = state.x2;
		k107 = state.x3;
		state.sbox0(w[108], w[109], w[110], w[111]);
		k108 = state.x0;
		k109 = state.x1;
		k110 = state.x2;
		k111 = state.x3;
		state.sbox7(w[112], w[113], w[114], w[115]);
		k112 = state.x0;
		k113 = state.x1;
		k114 = state.x2;
		k115 = state.x3;
		state.sbox6(w[116], w[117], w[118], w[119]);
		k116 = state.x0;
		k117 = state.x1;
		k118 = state.x2;
		k119 = state.x3;
		state.sbox5(w[120], w[121], w[122], w[123]);
		k120 = state.x0;
		k121 = state.x1;
		k122 = state.x2;
		k123 = state.x3;
		state.sbox4(w[124], w[125], w[126], w[127]);
		k124 = state.x0;
		k125 = state.x1;
		k126 = state.x2;
		k127 = state.x3;
		state.sbox3(w[128], w[129], w[130], w[131]);
		k128 = state.x0;
		k129 = state.x1;
		k130 = state.x2;
		k131 = state.x3;
	}


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the plaintext to be encrypted.
	 * @param inOffset Index in the in buffer where plaintext should be read.
	 * @param out A buffer where ciphertext is written.
	 * @param outOffset Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
	{
		State state = new State();
		state.x0 = (in[inOffset] & 0xff) | (in[inOffset + 1] & 0xff) << 8 | (in[inOffset + 2] & 0xff) << 16 | (in[inOffset + 3] & 0xff) << 24;
		state.x1 = (in[inOffset + 4] & 0xff) | (in[inOffset + 5] & 0xff) << 8 | (in[inOffset + 6] & 0xff) << 16 | (in[inOffset + 7] & 0xff) << 24;
		state.x2 = (in[inOffset + 8] & 0xff) | (in[inOffset + 9] & 0xff) << 8 | (in[inOffset + 10] & 0xff) << 16 | (in[inOffset + 11] & 0xff) << 24;
		state.x3 = (in[inOffset + 12] & 0xff) | (in[inOffset + 13] & 0xff) << 8 | (in[inOffset + 14] & 0xff) << 16 | (in[inOffset + 15] & 0xff) << 24;

		state.x0 ^= k0;
		state.x1 ^= k1;
		state.x2 ^= k2;
		state.x3 ^= k3;
		state.sbox0();
		state.x1 ^= k4;
		state.x4 ^= k5;
		state.x2 ^= k6;
		state.x0 ^= k7;
		state.sbox1();
		state.x0 ^= k8;
		state.x4 ^= k9;
		state.x2 ^= k10;
		state.x1 ^= k11;
		state.sbox2();
		state.x2 ^= k12;
		state.x1 ^= k13;
		state.x4 ^= k14;
		state.x3 ^= k15;
		state.sbox3();
		state.x1 ^= k16;
		state.x4 ^= k17;
		state.x3 ^= k18;
		state.x0 ^= k19;
		state.sbox4();
		state.x4 ^= k20;
		state.x2 ^= k21;
		state.x1 ^= k22;
		state.x0 ^= k23;
		state.sbox5();
		state.x2 ^= k24;
		state.x0 ^= k25;
		state.x4 ^= k26;
		state.x1 ^= k27;
		state.sbox6();
		state.x2 ^= k28;
		state.x0 ^= k29;
		state.x3 ^= k30;
		state.x4 ^= k31;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k32;
		state.x1 ^= k33;
		state.x2 ^= k34;
		state.x3 ^= k35;
		state.sbox0();
		state.x1 ^= k36;
		state.x4 ^= k37;
		state.x2 ^= k38;
		state.x0 ^= k39;
		state.sbox1();
		state.x0 ^= k40;
		state.x4 ^= k41;
		state.x2 ^= k42;
		state.x1 ^= k43;
		state.sbox2();
		state.x2 ^= k44;
		state.x1 ^= k45;
		state.x4 ^= k46;
		state.x3 ^= k47;
		state.sbox3();
		state.x1 ^= k48;
		state.x4 ^= k49;
		state.x3 ^= k50;
		state.x0 ^= k51;
		state.sbox4();
		state.x4 ^= k52;
		state.x2 ^= k53;
		state.x1 ^= k54;
		state.x0 ^= k55;
		state.sbox5();
		state.x2 ^= k56;
		state.x0 ^= k57;
		state.x4 ^= k58;
		state.x1 ^= k59;
		state.sbox6();
		state.x2 ^= k60;
		state.x0 ^= k61;
		state.x3 ^= k62;
		state.x4 ^= k63;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k64;
		state.x1 ^= k65;
		state.x2 ^= k66;
		state.x3 ^= k67;
		state.sbox0();
		state.x1 ^= k68;
		state.x4 ^= k69;
		state.x2 ^= k70;
		state.x0 ^= k71;
		state.sbox1();
		state.x0 ^= k72;
		state.x4 ^= k73;
		state.x2 ^= k74;
		state.x1 ^= k75;
		state.sbox2();
		state.x2 ^= k76;
		state.x1 ^= k77;
		state.x4 ^= k78;
		state.x3 ^= k79;
		state.sbox3();
		state.x1 ^= k80;
		state.x4 ^= k81;
		state.x3 ^= k82;
		state.x0 ^= k83;
		state.sbox4();
		state.x4 ^= k84;
		state.x2 ^= k85;
		state.x1 ^= k86;
		state.x0 ^= k87;
		state.sbox5();
		state.x2 ^= k88;
		state.x0 ^= k89;
		state.x4 ^= k90;
		state.x1 ^= k91;
		state.sbox6();
		state.x2 ^= k92;
		state.x0 ^= k93;
		state.x3 ^= k94;
		state.x4 ^= k95;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k96;
		state.x1 ^= k97;
		state.x2 ^= k98;
		state.x3 ^= k99;
		state.sbox0();
		state.x1 ^= k100;
		state.x4 ^= k101;
		state.x2 ^= k102;
		state.x0 ^= k103;
		state.sbox1();
		state.x0 ^= k104;
		state.x4 ^= k105;
		state.x2 ^= k106;
		state.x1 ^= k107;
		state.sbox2();
		state.x2 ^= k108;
		state.x1 ^= k109;
		state.x4 ^= k110;
		state.x3 ^= k111;
		state.sbox3();
		state.x1 ^= k112;
		state.x4 ^= k113;
		state.x3 ^= k114;
		state.x0 ^= k115;
		state.sbox4();
		state.x4 ^= k116;
		state.x2 ^= k117;
		state.x1 ^= k118;
		state.x0 ^= k119;
		state.sbox5();
		state.x2 ^= k120;
		state.x0 ^= k121;
		state.x4 ^= k122;
		state.x1 ^= k123;
		state.sbox6();
		state.x2 ^= k124;
		state.x0 ^= k125;
		state.x3 ^= k126;
		state.x4 ^= k127;
		state.sbox7noLT();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;
		state.x0 ^= k128;
		state.x1 ^= k129;
		state.x2 ^= k130;
		state.x3 ^= k131;

		out[outOffset] = (byte)state.x0;
		out[outOffset + 1] = (byte)(state.x0 >>> 8);
		out[outOffset + 2] = (byte)(state.x0 >>> 16);
		out[outOffset + 3] = (byte)(state.x0 >>> 24);
		out[outOffset + 4] = (byte)state.x1;
		out[outOffset + 5] = (byte)(state.x1 >>> 8);
		out[outOffset + 6] = (byte)(state.x1 >>> 16);
		out[outOffset + 7] = (byte)(state.x1 >>> 24);
		out[outOffset + 8] = (byte)state.x2;
		out[outOffset + 9] = (byte)(state.x2 >>> 8);
		out[outOffset + 10] = (byte)(state.x2 >>> 16);
		out[outOffset + 11] = (byte)(state.x2 >>> 24);
		out[outOffset + 12] = (byte)state.x3;
		out[outOffset + 13] = (byte)(state.x3 >>> 8);
		out[outOffset + 14] = (byte)(state.x3 >>> 16);
		out[outOffset + 15] = (byte)(state.x3 >>> 24);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the ciphertext to be decrypted.
	 * @param inOffset Index in the in buffer where ciphertext should be read.
	 * @param out A buffer where plaintext is written.
	 * @param outOffset Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(byte[] in, int inOffset, byte[] out, int outOffset)
	{
		State state = new State();
		state.x0 = (in[inOffset] & 0xff) | (in[inOffset + 1] & 0xff) << 8 | (in[inOffset + 2] & 0xff) << 16 | (in[inOffset + 3] & 0xff) << 24;
		state.x1 = (in[inOffset + 4] & 0xff) | (in[inOffset + 5] & 0xff) << 8 | (in[inOffset + 6] & 0xff) << 16 | (in[inOffset + 7] & 0xff) << 24;
		state.x2 = (in[inOffset + 8] & 0xff) | (in[inOffset + 9] & 0xff) << 8 | (in[inOffset + 10] & 0xff) << 16 | (in[inOffset + 11] & 0xff) << 24;
		state.x3 = (in[inOffset + 12] & 0xff) | (in[inOffset + 13] & 0xff) << 8 | (in[inOffset + 14] & 0xff) << 16 | (in[inOffset + 15] & 0xff) << 24;

		state.x0 ^= k128;
		state.x1 ^= k129;
		state.x2 ^= k130;
		state.x3 ^= k131;
		state.sboxI7noLT();
		state.x3 ^= k124;
		state.x0 ^= k125;
		state.x1 ^= k126;
		state.x4 ^= k127;
		state.sboxI6();
		state.x0 ^= k120;
		state.x1 ^= k121;
		state.x2 ^= k122;
		state.x4 ^= k123;
		state.sboxI5();
		state.x1 ^= k116;
		state.x3 ^= k117;
		state.x4 ^= k118;
		state.x2 ^= k119;
		state.sboxI4();
		state.x1 ^= k112;
		state.x2 ^= k113;
		state.x4 ^= k114;
		state.x0 ^= k115;
		state.sboxI3();
		state.x0 ^= k108;
		state.x1 ^= k109;
		state.x4 ^= k110;
		state.x2 ^= k111;
		state.sboxI2();
		state.x1 ^= k104;
		state.x3 ^= k105;
		state.x4 ^= k106;
		state.x2 ^= k107;
		state.sboxI1();
		state.x0 ^= k100;
		state.x1 ^= k101;
		state.x2 ^= k102;
		state.x4 ^= k103;
		state.sboxI0();
		state.x0 ^= k96;
		state.x3 ^= k97;
		state.x1 ^= k98;
		state.x4 ^= k99;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k92;
		state.x0 ^= k93;
		state.x1 ^= k94;
		state.x4 ^= k95;
		state.sboxI6();
		state.x0 ^= k88;
		state.x1 ^= k89;
		state.x2 ^= k90;
		state.x4 ^= k91;
		state.sboxI5();
		state.x1 ^= k84;
		state.x3 ^= k85;
		state.x4 ^= k86;
		state.x2 ^= k87;
		state.sboxI4();
		state.x1 ^= k80;
		state.x2 ^= k81;
		state.x4 ^= k82;
		state.x0 ^= k83;
		state.sboxI3();
		state.x0 ^= k76;
		state.x1 ^= k77;
		state.x4 ^= k78;
		state.x2 ^= k79;
		state.sboxI2();
		state.x1 ^= k72;
		state.x3 ^= k73;
		state.x4 ^= k74;
		state.x2 ^= k75;
		state.sboxI1();
		state.x0 ^= k68;
		state.x1 ^= k69;
		state.x2 ^= k70;
		state.x4 ^= k71;
		state.sboxI0();
		state.x0 ^= k64;
		state.x3 ^= k65;
		state.x1 ^= k66;
		state.x4 ^= k67;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k60;
		state.x0 ^= k61;
		state.x1 ^= k62;
		state.x4 ^= k63;
		state.sboxI6();
		state.x0 ^= k56;
		state.x1 ^= k57;
		state.x2 ^= k58;
		state.x4 ^= k59;
		state.sboxI5();
		state.x1 ^= k52;
		state.x3 ^= k53;
		state.x4 ^= k54;
		state.x2 ^= k55;
		state.sboxI4();
		state.x1 ^= k48;
		state.x2 ^= k49;
		state.x4 ^= k50;
		state.x0 ^= k51;
		state.sboxI3();
		state.x0 ^= k44;
		state.x1 ^= k45;
		state.x4 ^= k46;
		state.x2 ^= k47;
		state.sboxI2();
		state.x1 ^= k40;
		state.x3 ^= k41;
		state.x4 ^= k42;
		state.x2 ^= k43;
		state.sboxI1();
		state.x0 ^= k36;
		state.x1 ^= k37;
		state.x2 ^= k38;
		state.x4 ^= k39;
		state.sboxI0();
		state.x0 ^= k32;
		state.x3 ^= k33;
		state.x1 ^= k34;
		state.x4 ^= k35;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k28;
		state.x0 ^= k29;
		state.x1 ^= k30;
		state.x4 ^= k31;
		state.sboxI6();
		state.x0 ^= k24;
		state.x1 ^= k25;
		state.x2 ^= k26;
		state.x4 ^= k27;
		state.sboxI5();
		state.x1 ^= k20;
		state.x3 ^= k21;
		state.x4 ^= k22;
		state.x2 ^= k23;
		state.sboxI4();
		state.x1 ^= k16;
		state.x2 ^= k17;
		state.x4 ^= k18;
		state.x0 ^= k19;
		state.sboxI3();
		state.x0 ^= k12;
		state.x1 ^= k13;
		state.x4 ^= k14;
		state.x2 ^= k15;
		state.sboxI2();
		state.x1 ^= k8;
		state.x3 ^= k9;
		state.x4 ^= k10;
		state.x2 ^= k11;
		state.sboxI1();
		state.x0 ^= k4;
		state.x1 ^= k5;
		state.x2 ^= k6;
		state.x4 ^= k7;
		state.sboxI0();
		state.x2 = state.x1;
		state.x1 = state.x3;
		state.x3 = state.x4;

		state.x0 ^= k0;
		state.x1 ^= k1;
		state.x2 ^= k2;
		state.x3 ^= k3;

		out[outOffset] = (byte)state.x0;
		out[outOffset + 1] = (byte)(state.x0 >>> 8);
		out[outOffset + 2] = (byte)(state.x0 >>> 16);
		out[outOffset + 3] = (byte)(state.x0 >>> 24);
		out[outOffset + 4] = (byte)state.x1;
		out[outOffset + 5] = (byte)(state.x1 >>> 8);
		out[outOffset + 6] = (byte)(state.x1 >>> 16);
		out[outOffset + 7] = (byte)(state.x1 >>> 24);
		out[outOffset + 8] = (byte)state.x2;
		out[outOffset + 9] = (byte)(state.x2 >>> 8);
		out[outOffset + 10] = (byte)(state.x2 >>> 16);
		out[outOffset + 11] = (byte)(state.x2 >>> 24);
		out[outOffset + 12] = (byte)state.x3;
		out[outOffset + 13] = (byte)(state.x3 >>> 8);
		out[outOffset + 14] = (byte)(state.x3 >>> 16);
		out[outOffset + 15] = (byte)(state.x3 >>> 24);
	}


	/**
	 * Encrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the plaintext to be encrypted.
	 * @param inOffset Index in the in buffer where plaintext should be read.
	 * @param out A buffer where ciphertext is written.
	 * @param outOffset Index in the out buffer where ciphertext should be written.
	 */
	@Override
	public void engineEncryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		State state = new State();
		state.x0 = reverseBytes(in[inOffset++]);
		state.x1 = reverseBytes(in[inOffset++]);
		state.x2 = reverseBytes(in[inOffset++]);
		state.x3 = reverseBytes(in[inOffset]);

		state.x0 ^= k0;
		state.x1 ^= k1;
		state.x2 ^= k2;
		state.x3 ^= k3;
		state.sbox0();
		state.x1 ^= k4;
		state.x4 ^= k5;
		state.x2 ^= k6;
		state.x0 ^= k7;
		state.sbox1();
		state.x0 ^= k8;
		state.x4 ^= k9;
		state.x2 ^= k10;
		state.x1 ^= k11;
		state.sbox2();
		state.x2 ^= k12;
		state.x1 ^= k13;
		state.x4 ^= k14;
		state.x3 ^= k15;
		state.sbox3();
		state.x1 ^= k16;
		state.x4 ^= k17;
		state.x3 ^= k18;
		state.x0 ^= k19;
		state.sbox4();
		state.x4 ^= k20;
		state.x2 ^= k21;
		state.x1 ^= k22;
		state.x0 ^= k23;
		state.sbox5();
		state.x2 ^= k24;
		state.x0 ^= k25;
		state.x4 ^= k26;
		state.x1 ^= k27;
		state.sbox6();
		state.x2 ^= k28;
		state.x0 ^= k29;
		state.x3 ^= k30;
		state.x4 ^= k31;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k32;
		state.x1 ^= k33;
		state.x2 ^= k34;
		state.x3 ^= k35;
		state.sbox0();
		state.x1 ^= k36;
		state.x4 ^= k37;
		state.x2 ^= k38;
		state.x0 ^= k39;
		state.sbox1();
		state.x0 ^= k40;
		state.x4 ^= k41;
		state.x2 ^= k42;
		state.x1 ^= k43;
		state.sbox2();
		state.x2 ^= k44;
		state.x1 ^= k45;
		state.x4 ^= k46;
		state.x3 ^= k47;
		state.sbox3();
		state.x1 ^= k48;
		state.x4 ^= k49;
		state.x3 ^= k50;
		state.x0 ^= k51;
		state.sbox4();
		state.x4 ^= k52;
		state.x2 ^= k53;
		state.x1 ^= k54;
		state.x0 ^= k55;
		state.sbox5();
		state.x2 ^= k56;
		state.x0 ^= k57;
		state.x4 ^= k58;
		state.x1 ^= k59;
		state.sbox6();
		state.x2 ^= k60;
		state.x0 ^= k61;
		state.x3 ^= k62;
		state.x4 ^= k63;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k64;
		state.x1 ^= k65;
		state.x2 ^= k66;
		state.x3 ^= k67;
		state.sbox0();
		state.x1 ^= k68;
		state.x4 ^= k69;
		state.x2 ^= k70;
		state.x0 ^= k71;
		state.sbox1();
		state.x0 ^= k72;
		state.x4 ^= k73;
		state.x2 ^= k74;
		state.x1 ^= k75;
		state.sbox2();
		state.x2 ^= k76;
		state.x1 ^= k77;
		state.x4 ^= k78;
		state.x3 ^= k79;
		state.sbox3();
		state.x1 ^= k80;
		state.x4 ^= k81;
		state.x3 ^= k82;
		state.x0 ^= k83;
		state.sbox4();
		state.x4 ^= k84;
		state.x2 ^= k85;
		state.x1 ^= k86;
		state.x0 ^= k87;
		state.sbox5();
		state.x2 ^= k88;
		state.x0 ^= k89;
		state.x4 ^= k90;
		state.x1 ^= k91;
		state.sbox6();
		state.x2 ^= k92;
		state.x0 ^= k93;
		state.x3 ^= k94;
		state.x4 ^= k95;
		state.sbox7();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;

		state.x0 ^= k96;
		state.x1 ^= k97;
		state.x2 ^= k98;
		state.x3 ^= k99;
		state.sbox0();
		state.x1 ^= k100;
		state.x4 ^= k101;
		state.x2 ^= k102;
		state.x0 ^= k103;
		state.sbox1();
		state.x0 ^= k104;
		state.x4 ^= k105;
		state.x2 ^= k106;
		state.x1 ^= k107;
		state.sbox2();
		state.x2 ^= k108;
		state.x1 ^= k109;
		state.x4 ^= k110;
		state.x3 ^= k111;
		state.sbox3();
		state.x1 ^= k112;
		state.x4 ^= k113;
		state.x3 ^= k114;
		state.x0 ^= k115;
		state.sbox4();
		state.x4 ^= k116;
		state.x2 ^= k117;
		state.x1 ^= k118;
		state.x0 ^= k119;
		state.sbox5();
		state.x2 ^= k120;
		state.x0 ^= k121;
		state.x4 ^= k122;
		state.x1 ^= k123;
		state.sbox6();
		state.x2 ^= k124;
		state.x0 ^= k125;
		state.x3 ^= k126;
		state.x4 ^= k127;
		state.sbox7noLT();
		state.x0 = state.x3;
		state.x3 = state.x2;
		state.x2 = state.x4;
		state.x0 ^= k128;
		state.x1 ^= k129;
		state.x2 ^= k130;
		state.x3 ^= k131;

		out[outOffset++] = reverseBytes(state.x0);
		out[outOffset++] = reverseBytes(state.x1);
		out[outOffset++] = reverseBytes(state.x2);
		out[outOffset] = reverseBytes(state.x3);
	}


	/**
	 * Decrypts a single block of ciphertext in ECB-mode.
	 *
	 * @param in A buffer containing the ciphertext to be decrypted.
	 * @param inOffset Index in the in buffer where ciphertext should be read.
	 * @param out A buffer where plaintext is written.
	 * @param outOffset Index in the out buffer where plaintext should be written.
	 */
	@Override
	public void engineDecryptBlock(int[] in, int inOffset, int[] out, int outOffset)
	{
		State state = new State();
		state.x0 = reverseBytes(in[inOffset++]);
		state.x1 = reverseBytes(in[inOffset++]);
		state.x2 = reverseBytes(in[inOffset++]);
		state.x3 = reverseBytes(in[inOffset]);

		state.x0 ^= k128;
		state.x1 ^= k129;
		state.x2 ^= k130;
		state.x3 ^= k131;
		state.sboxI7noLT();
		state.x3 ^= k124;
		state.x0 ^= k125;
		state.x1 ^= k126;
		state.x4 ^= k127;
		state.sboxI6();
		state.x0 ^= k120;
		state.x1 ^= k121;
		state.x2 ^= k122;
		state.x4 ^= k123;
		state.sboxI5();
		state.x1 ^= k116;
		state.x3 ^= k117;
		state.x4 ^= k118;
		state.x2 ^= k119;
		state.sboxI4();
		state.x1 ^= k112;
		state.x2 ^= k113;
		state.x4 ^= k114;
		state.x0 ^= k115;
		state.sboxI3();
		state.x0 ^= k108;
		state.x1 ^= k109;
		state.x4 ^= k110;
		state.x2 ^= k111;
		state.sboxI2();
		state.x1 ^= k104;
		state.x3 ^= k105;
		state.x4 ^= k106;
		state.x2 ^= k107;
		state.sboxI1();
		state.x0 ^= k100;
		state.x1 ^= k101;
		state.x2 ^= k102;
		state.x4 ^= k103;
		state.sboxI0();
		state.x0 ^= k96;
		state.x3 ^= k97;
		state.x1 ^= k98;
		state.x4 ^= k99;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k92;
		state.x0 ^= k93;
		state.x1 ^= k94;
		state.x4 ^= k95;
		state.sboxI6();
		state.x0 ^= k88;
		state.x1 ^= k89;
		state.x2 ^= k90;
		state.x4 ^= k91;
		state.sboxI5();
		state.x1 ^= k84;
		state.x3 ^= k85;
		state.x4 ^= k86;
		state.x2 ^= k87;
		state.sboxI4();
		state.x1 ^= k80;
		state.x2 ^= k81;
		state.x4 ^= k82;
		state.x0 ^= k83;
		state.sboxI3();
		state.x0 ^= k76;
		state.x1 ^= k77;
		state.x4 ^= k78;
		state.x2 ^= k79;
		state.sboxI2();
		state.x1 ^= k72;
		state.x3 ^= k73;
		state.x4 ^= k74;
		state.x2 ^= k75;
		state.sboxI1();
		state.x0 ^= k68;
		state.x1 ^= k69;
		state.x2 ^= k70;
		state.x4 ^= k71;
		state.sboxI0();
		state.x0 ^= k64;
		state.x3 ^= k65;
		state.x1 ^= k66;
		state.x4 ^= k67;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k60;
		state.x0 ^= k61;
		state.x1 ^= k62;
		state.x4 ^= k63;
		state.sboxI6();
		state.x0 ^= k56;
		state.x1 ^= k57;
		state.x2 ^= k58;
		state.x4 ^= k59;
		state.sboxI5();
		state.x1 ^= k52;
		state.x3 ^= k53;
		state.x4 ^= k54;
		state.x2 ^= k55;
		state.sboxI4();
		state.x1 ^= k48;
		state.x2 ^= k49;
		state.x4 ^= k50;
		state.x0 ^= k51;
		state.sboxI3();
		state.x0 ^= k44;
		state.x1 ^= k45;
		state.x4 ^= k46;
		state.x2 ^= k47;
		state.sboxI2();
		state.x1 ^= k40;
		state.x3 ^= k41;
		state.x4 ^= k42;
		state.x2 ^= k43;
		state.sboxI1();
		state.x0 ^= k36;
		state.x1 ^= k37;
		state.x2 ^= k38;
		state.x4 ^= k39;
		state.sboxI0();
		state.x0 ^= k32;
		state.x3 ^= k33;
		state.x1 ^= k34;
		state.x4 ^= k35;
		state.sboxI7();
		state.x1 = state.x3;
		state.x3 = state.x4;
		state.x4 = state.x2;

		state.x3 ^= k28;
		state.x0 ^= k29;
		state.x1 ^= k30;
		state.x4 ^= k31;
		state.sboxI6();
		state.x0 ^= k24;
		state.x1 ^= k25;
		state.x2 ^= k26;
		state.x4 ^= k27;
		state.sboxI5();
		state.x1 ^= k20;
		state.x3 ^= k21;
		state.x4 ^= k22;
		state.x2 ^= k23;
		state.sboxI4();
		state.x1 ^= k16;
		state.x2 ^= k17;
		state.x4 ^= k18;
		state.x0 ^= k19;
		state.sboxI3();
		state.x0 ^= k12;
		state.x1 ^= k13;
		state.x4 ^= k14;
		state.x2 ^= k15;
		state.sboxI2();
		state.x1 ^= k8;
		state.x3 ^= k9;
		state.x4 ^= k10;
		state.x2 ^= k11;
		state.sboxI1();
		state.x0 ^= k4;
		state.x1 ^= k5;
		state.x2 ^= k6;
		state.x4 ^= k7;
		state.sboxI0();
		state.x2 = state.x1;
		state.x1 = state.x3;
		state.x3 = state.x4;

		state.x0 ^= k0;
		state.x1 ^= k1;
		state.x2 ^= k2;
		state.x3 ^= k3;

		out[outOffset++] = reverseBytes(state.x0);
		out[outOffset++] = reverseBytes(state.x1);
		out[outOffset++] = reverseBytes(state.x2);
		out[outOffset] = reverseBytes(state.x3);
	}


	/**
	 * Resets all internal state data. This Cipher object needs to be reinitialized again before it can be used again.
	 */
	@Override
	public void engineReset()
	{
		k0 = k1 = k2 = k3 = k4 = k5 = k6 = k7 = k8 = k9 = k10 = k11 = k12 = k13
			= k14 = k15 = k16 = k17 = k18 = k19 = k20 = k21 = k22 = k23 = k24 = k25 = k26
			= k27 = k28 = k29 = k30 = k31 = k32 = k33 = k34 = k35 = k36 = k37 = k38 = k39
			= k40 = k41 = k42 = k43 = k44 = k45 = k46 = k47 = k48 = k49 = k50 = k51 = k52
			= k53 = k54 = k55 = k56 = k57 = k58 = k59 = k60 = k61 = k62 = k63 = k64 = k65
			= k66 = k67 = k68 = k69 = k70 = k71 = k72 = k73 = k74 = k75 = k76 = k77 = k78
			= k79 = k80 = k81 = k82 = k83 = k84 = k85 = k86 = k87 = k88 = k89 = k90 = k91
			= k92 = k93 = k94 = k95 = k96 = k97 = k98 = k99 = k100 = k101 = k102 = k103
			= k104 = k105 = k106 = k107 = k108 = k109 = k110 = k111 = k112 = k113 = k114
			= k115 = k116 = k117 = k118 = k119 = k120 = k121 = k122 = k123 = k124 = k125
			= k126 = k127 = k128 = k129 = k130 = k131 = 0;
	}


	@Override
	public String toString()
	{
		return "Serpent";
	}


	private static int reverseBytes(int i)
	{
		return ((i >>> 24))
			+ ((i >> 8) & 0xFF00)
			+ ((i << 8) & 0xFF0000)
			+ ((i << 24));
	}


	private static class State
	{
		transient int x0, x1, x2, x3, x4;


		private void sbox0()
		{
			x3 ^= x0;
			x4 = x1;
			x1 &= x3;
			x4 ^= x2;
			x1 ^= x0;
			x0 |= x3;
			x0 ^= x4;
			x4 ^= x3;
			x3 ^= x2;
			x2 |= x1;
			x2 ^= x4;
			x4 ^= -1;
			x4 |= x1;
			x1 ^= x3;
			x1 ^= x4;
			x3 |= x0;
			x1 ^= x3;
			x4 ^= x3;

			x1 = (x1 << 13) | (x1 >>> 19);
			x4 ^= x1;
			x3 = x1 << 3;
			x2 = (x2 << 3) | (x2 >>> 29);
			x4 ^= x2;
			x0 ^= x2;
			x4 = (x4 << 1) | (x4 >>> 31);
			x0 ^= x3;
			x0 = (x0 << 7) | (x0 >>> 25);
			x3 = x4;
			x1 ^= x4;
			x3 <<= 7;
			x1 ^= x0;
			x2 ^= x0;
			x2 ^= x3;
			x1 = (x1 << 5) | (x1 >>> 27);
			x2 = (x2 << 22) | (x2 >>> 10);
		}


		private void sbox1()
		{
			x4 = ~x4;
			x3 = x1;
			x1 ^= x4;
			x3 |= x4;
			x3 ^= x0;
			x0 &= x1;
			x2 ^= x3;
			x0 ^= x4;
			x0 |= x2;
			x1 ^= x3;
			x0 ^= x1;
			x4 &= x2;
			x1 |= x4;
			x4 ^= x3;
			x1 ^= x2;
			x3 |= x0;
			x1 ^= x3;
			x3 = ~x3;
			x4 ^= x0;
			x3 &= x2;
			x4 = ~x4;
			x3 ^= x1;
			x4 ^= x3;

			x0 = (x0 << 13) | (x0 >>> 19);
			x4 ^= x0;
			x3 = x0 << 3;
			x2 = (x2 << 3) | (x2 >>> 29);
			x4 ^= x2;
			x1 ^= x2;
			x4 = (x4 << 1) | (x4 >>> 31);
			x1 ^= x3;
			x1 = (x1 << 7) | (x1 >>> 25);
			x3 = x4;
			x0 ^= x4;
			x3 <<= 7;
			x0 ^= x1;
			x2 ^= x1;
			x2 ^= x3;
			x0 = (x0 << 5) | (x0 >>> 27);
			x2 = (x2 << 22) | (x2 >>> 10);
		}


		private void sbox2()
		{
			x3 = x0;
			x0 = x0 & x2;
			x0 = x0 ^ x1;
			x2 = x2 ^ x4;
			x2 = x2 ^ x0;
			x1 = x1 | x3;
			x1 = x1 ^ x4;
			x3 = x3 ^ x2;
			x4 = x1;
			x1 = x1 | x3;
			x1 = x1 ^ x0;
			x0 = x0 & x4;
			x3 = x3 ^ x0;
			x4 = x4 ^ x1;
			x4 = x4 ^ x3;
			x3 = ~x3;

			x2 = (x2 << 13) | (x2 >>> 19);
			x1 ^= x2;
			x0 = x2 << 3;
			x4 = (x4 << 3) | (x4 >>> 29);
			x1 ^= x4;
			x3 ^= x4;
			x1 = (x1 << 1) | (x1 >>> 31);
			x3 ^= x0;
			x3 = (x3 << 7) | (x3 >>> 25);
			x0 = x1;
			x2 ^= x1;
			x0 <<= 7;
			x2 ^= x3;
			x4 ^= x3;
			x4 ^= x0;
			x2 = (x2 << 5) | (x2 >>> 27);
			x4 = (x4 << 22) | (x4 >>> 10);
		}


		private void sbox3()
		{
			x0 = x2;
			x2 = x2 | x3;
			x3 = x3 ^ x1;
			x1 = x1 & x0;
			x0 = x0 ^ x4;
			x4 = x4 ^ x3;
			x3 = x3 & x2;
			x0 = x0 | x1;
			x3 = x3 ^ x0;
			x2 = x2 ^ x1;
			x0 = x0 & x2;
			x1 = x1 ^ x3;
			x0 = x0 ^ x4;
			x1 = x1 | x2;
			x1 = x1 ^ x4;
			x2 = x2 ^ x3;
			x4 = x1;
			x1 = x1 | x3;
			x1 = x1 ^ x2;

			x1 = (x1 << 13) | (x1 >>> 19);
			x4 ^= x1;
			x2 = x1 << 3;
			x3 = (x3 << 3) | (x3 >>> 29);
			x4 ^= x3;
			x0 ^= x3;
			x4 = (x4 << 1) | (x4 >>> 31);
			x0 ^= x2;
			x0 = (x0 << 7) | (x0 >>> 25);
			x2 = x4;
			x1 ^= x4;
			x2 <<= 7;
			x1 ^= x0;
			x3 ^= x0;
			x3 ^= x2;
			x1 = (x1 << 5) | (x1 >>> 27);
			x3 = (x3 << 22) | (x3 >>> 10);
		}


		private void sbox4()
		{
			x4 = x4 ^ x0;
			x0 = ~x0;
			x3 = x3 ^ x0;
			x0 = x0 ^ x1;
			x2 = x4;
			x4 = x4 & x0;
			x4 = x4 ^ x3;
			x2 = x2 ^ x0;
			x1 = x1 ^ x2;
			x3 = x3 & x2;
			x3 = x3 ^ x1;
			x1 = x1 & x4;
			x0 = x0 ^ x1;
			x2 = x2 | x4;
			x2 = x2 ^ x1;
			x1 = x1 | x0;
			x1 = x1 ^ x3;
			x3 = x3 & x0;
			x1 = ~x1;
			x2 = x2 ^ x3;

			x4 = (x4 << 13) | (x4 >>> 19);
			x2 ^= x4;
			x3 = x4 << 3;
			x1 = (x1 << 3) | (x1 >>> 29);
			x2 ^= x1;
			x0 ^= x1;
			x2 = (x2 << 1) | (x2 >>> 31);
			x0 ^= x3;
			x0 = (x0 << 7) | (x0 >>> 25);
			x3 = x2;
			x4 ^= x2;
			x3 <<= 7;
			x4 ^= x0;
			x1 ^= x0;
			x1 ^= x3;
			x4 = (x4 << 5) | (x4 >>> 27);
			x1 = (x1 << 22) | (x1 >>> 10);
		}


		private void sbox5()
		{
			x4 = x4 ^ x2;
			x2 = x2 ^ x0;
			x0 = ~x0;
			x3 = x2;
			x2 = x2 & x4;
			x1 = x1 ^ x0;
			x2 = x2 ^ x1;
			x1 = x1 | x3;
			x3 = x3 ^ x0;
			x0 = x0 & x2;
			x0 = x0 ^ x4;
			x3 = x3 ^ x2;
			x3 = x3 ^ x1;
			x1 = x1 ^ x4;
			x4 = x4 & x0;
			x1 = ~x1;
			x4 = x4 ^ x3;
			x3 = x3 | x0;
			x1 = x1 ^ x3;

			x2 = (x2 << 13) | (x2 >>> 19);
			x0 ^= x2;
			x3 = x2 << 3;
			x4 = (x4 << 3) | (x4 >>> 29);
			x0 ^= x4;
			x1 ^= x4;
			x0 = (x0 << 1) | (x0 >>> 31);
			x1 ^= x3;
			x1 = (x1 << 7) | (x1 >>> 25);
			x3 = x0;
			x2 ^= x0;
			x3 <<= 7;
			x2 ^= x1;
			x4 ^= x1;
			x4 ^= x3;
			x2 = (x2 << 5) | (x2 >>> 27);
			x4 = (x4 << 22) | (x4 >>> 10);
		}


		private void sbox6()
		{
			x4 = ~x4;
			x3 = x1;
			x1 = x1 & x2;
			x2 = x2 ^ x3;
			x1 = x1 ^ x4;
			x4 = x4 | x3;
			x0 = x0 ^ x1;
			x4 = x4 ^ x2;
			x2 = x2 | x0;
			x4 = x4 ^ x0;
			x3 = x3 ^ x2;
			x2 = x2 | x1;
			x2 = x2 ^ x4;
			x3 = x3 ^ x1;
			x3 = x3 ^ x2;
			x1 = ~x1;
			x4 = x4 & x3;
			x4 = x4 ^ x1;
			x2 = (x2 << 13) | (x2 >>> 19);
			x0 ^= x2;
			x1 = x2 << 3;
			x3 = (x3 << 3) | (x3 >>> 29);
			x0 ^= x3;
			x4 ^= x3;
			x0 = (x0 << 1) | (x0 >>> 31);
			x4 ^= x1;
			x4 = (x4 << 7) | (x4 >>> 25);
			x1 = x0;
			x2 ^= x0;
			x1 <<= 7;
			x2 ^= x4;
			x3 ^= x4;
			x3 ^= x1;
			x2 = (x2 << 5) | (x2 >>> 27);
			x3 = (x3 << 22) | (x3 >>> 10);
		}


		private void sbox7()
		{
			x1 = x3;
			x3 = x3 & x0;
			x3 = x3 ^ x4;
			x4 = x4 & x0;
			x1 = x1 ^ x3;
			x3 = x3 ^ x0;
			x0 = x0 ^ x2;
			x2 = x2 | x1;
			x2 = x2 ^ x3;
			x4 = x4 ^ x0;
			x3 = x3 ^ x4;
			x4 = x4 & x2;
			x4 = x4 ^ x1;
			x1 = x1 ^ x3;
			x3 = x3 & x2;
			x1 = ~x1;
			x3 = x3 ^ x1;
			x1 = x1 & x2;
			x0 = x0 ^ x4;
			x1 = x1 ^ x0;
			x3 = (x3 << 13) | (x3 >>> 19);
			x1 ^= x3;
			x0 = x3 << 3;
			x4 = (x4 << 3) | (x4 >>> 29);
			x1 ^= x4;
			x2 ^= x4;
			x1 = (x1 << 1) | (x1 >>> 31);
			x2 ^= x0;
			x2 = (x2 << 7) | (x2 >>> 25);
			x0 = x1;
			x3 ^= x1;
			x0 <<= 7;
			x3 ^= x2;
			x4 ^= x2;
			x4 ^= x0;
			x3 = (x3 << 5) | (x3 >>> 27);
			x4 = (x4 << 22) | (x4 >>> 10);
		}


		/**
		 * The final S-box, with no transform.
		 */
		private void sbox7noLT()
		{
			x1 = x3;
			x3 = x3 & x0;
			x3 = x3 ^ x4;
			x4 = x4 & x0;
			x1 = x1 ^ x3;
			x3 = x3 ^ x0;
			x0 = x0 ^ x2;
			x2 = x2 | x1;
			x2 = x2 ^ x3;
			x4 = x4 ^ x0;
			x3 = x3 ^ x4;
			x4 = x4 & x2;
			x4 = x4 ^ x1;
			x1 = x1 ^ x3;
			x3 = x3 & x2;
			x1 = ~x1;
			x3 = x3 ^ x1;
			x1 = x1 & x2;
			x0 = x0 ^ x4;
			x1 = x1 ^ x0;
		}


		private void sboxI7noLT()
		{
			x4 = x2;
			x2 ^= x0;
			x0 &= x3;
			x2 = ~x2;
			x4 |= x3;
			x3 ^= x1;
			x1 |= x0;
			x0 ^= x2;
			x2 &= x4;
			x1 ^= x2;
			x2 ^= x0;
			x0 |= x2;
			x3 &= x4;
			x0 ^= x3;
			x4 ^= x1;
			x3 ^= x4;
			x4 |= x0;
			x3 ^= x2;
			x4 ^= x2;
		}


		private void sboxI6()
		{
			x1 = (x1 >>> 22) | (x1 << 10);
			x3 = (x3 >>> 5) | (x3 << 27);
			x2 = x0;
			x1 ^= x4;
			x2 <<= 7;
			x3 ^= x4;
			x1 ^= x2;
			x3 ^= x0;
			x4 = (x4 >>> 7) | (x4 << 25);
			x0 = (x0 >>> 1) | (x0 << 31);
			x0 ^= x3;
			x2 = x3 << 3;
			x4 ^= x2;
			x3 = (x3 >>> 13) | (x3 << 19);
			x0 ^= x1;
			x4 ^= x1;
			x1 = (x1 >>> 3) | (x1 << 29);
			x3 ^= x1;
			x2 = x1;
			x1 &= x3;
			x2 ^= x4;
			x1 = ~x1;
			x4 ^= x0;
			x1 ^= x4;
			x2 |= x3;
			x3 ^= x1;
			x4 ^= x2;
			x2 ^= x0;
			x0 &= x4;
			x0 ^= x3;
			x3 ^= x4;
			x3 |= x1;
			x4 ^= x0;
			x2 ^= x3;
		}


		private void sboxI5()
		{
			x2 = (x2 >>> 22) | (x2 << 10);
			x0 = (x0 >>> 5) | (x0 << 27);
			x3 = x1;
			x2 ^= x4;
			x3 <<= 7;
			x0 ^= x4;
			x2 ^= x3;
			x0 ^= x1;
			x4 = (x4 >>> 7) | (x4 << 25);
			x1 = (x1 >>> 1) | (x1 << 31);
			x1 ^= x0;
			x3 = x0 << 3;
			x4 ^= x3;
			x0 = (x0 >>> 13) | (x0 << 19);
			x1 ^= x2;
			x4 ^= x2;
			x2 = (x2 >>> 3) | (x2 << 29);
			x1 = ~x1;
			x3 = x4;
			x2 ^= x1;
			x4 |= x0;
			x4 ^= x2;
			x2 |= x1;
			x2 &= x0;
			x3 ^= x4;
			x2 ^= x3;
			x3 |= x0;
			x3 ^= x1;
			x1 &= x2;
			x1 ^= x4;
			x3 ^= x2;
			x4 &= x3;
			x3 ^= x1;
			x4 ^= x0;
			x4 ^= x3;
			x3 = ~x3;
		}


		private void sboxI4()
		{
			x4 = (x4 >>> 22) | (x4 << 10);
			x1 = (x1 >>> 5) | (x1 << 27);
			x0 = x3;
			x4 ^= x2;
			x0 <<= 7;
			x1 ^= x2;
			x4 ^= x0;
			x1 ^= x3;
			x2 = (x2 >>> 7) | (x2 << 25);
			x3 = (x3 >>> 1) | (x3 << 31);
			x3 ^= x1;
			x0 = x1 << 3;
			x2 ^= x0;
			x1 = (x1 >>> 13) | (x1 << 19);
			x3 ^= x4;
			x2 ^= x4;
			x4 = (x4 >>> 3) | (x4 << 29);
			x0 = x4;
			x4 &= x2;
			x4 ^= x3;
			x3 |= x2;
			x3 &= x1;
			x0 ^= x4;
			x0 ^= x3;
			x3 &= x4;
			x1 = ~x1;
			x2 ^= x0;
			x3 ^= x2;
			x2 &= x1;
			x2 ^= x4;
			x1 ^= x3;
			x4 &= x1;
			x2 ^= x1;
			x4 ^= x0;
			x4 |= x2;
			x2 ^= x1;
			x4 ^= x3;
		}


		private void sboxI3()
		{
			x4 = (x4 >>> 22) | (x4 << 10);
			x1 = (x1 >>> 5) | (x1 << 27);
			x3 = x2;
			x4 ^= x0;
			x3 <<= 7;
			x1 ^= x0;
			x4 ^= x3;
			x1 ^= x2;
			x0 = (x0 >>> 7) | (x0 << 25);
			x2 = (x2 >>> 1) | (x2 << 31);
			x2 ^= x1;
			x3 = x1 << 3;
			x0 ^= x3;
			x1 = (x1 >>> 13) | (x1 << 19);
			x2 ^= x4;
			x0 ^= x4;
			x4 = (x4 >>> 3) | (x4 << 29);
			x3 = x4;
			x4 ^= x2;
			x2 &= x4;
			x2 ^= x1;
			x1 &= x3;
			x3 ^= x0;
			x0 |= x2;
			x0 ^= x4;
			x1 ^= x3;
			x4 ^= x1;
			x1 |= x0;
			x1 ^= x2;
			x3 ^= x4;
			x4 &= x0;
			x2 |= x0;
			x2 ^= x4;
			x3 ^= x1;
			x4 ^= x3;
		}


		private void sboxI2()
		{
			x4 = (x4 >>> 22) | (x4 << 10);
			x0 = (x0 >>> 5) | (x0 << 27);
			x3 = x1;
			x4 ^= x2;
			x3 <<= 7;
			x0 ^= x2;
			x4 ^= x3;
			x0 ^= x1;
			x2 = (x2 >>> 7) | (x2 << 25);
			x1 = (x1 >>> 1) | (x1 << 31);
			x1 ^= x0;
			x3 = x0 << 3;
			x2 ^= x3;
			x0 = (x0 >>> 13) | (x0 << 19);
			x1 ^= x4;
			x2 ^= x4;
			x4 = (x4 >>> 3) | (x4 << 29);
			x4 ^= x2;
			x2 ^= x0;
			x3 = x2;
			x2 &= x4;
			x2 ^= x1;
			x1 |= x4;
			x1 ^= x3;
			x3 &= x2;
			x4 ^= x2;
			x3 &= x0;
			x3 ^= x4;
			x4 &= x1;
			x4 |= x0;
			x2 = ~x2;
			x4 ^= x2;
			x0 ^= x2;
			x0 &= x1;
			x2 ^= x3;
			x2 ^= x0;
		}


		private void sboxI1()
		{
			x4 = (x4 >>> 22) | (x4 << 10);
			x1 = (x1 >>> 5) | (x1 << 27);
			x0 = x3;
			x4 ^= x2;
			x0 <<= 7;
			x1 ^= x2;
			x4 ^= x0;
			x1 ^= x3;
			x2 = (x2 >>> 7) | (x2 << 25);
			x3 = (x3 >>> 1) | (x3 << 31);
			x3 ^= x1;
			x0 = x1 << 3;
			x2 ^= x0;
			x1 = (x1 >>> 13) | (x1 << 19);
			x3 ^= x4;
			x2 ^= x4;
			x4 = (x4 >>> 3) | (x4 << 29);
			x0 = x3;
			x3 ^= x2;
			x2 &= x3;
			x0 ^= x4;
			x2 ^= x1;
			x1 |= x3;
			x4 ^= x2;
			x1 ^= x0;
			x1 |= x4;
			x3 ^= x2;
			x1 ^= x3;
			x3 |= x2;
			x3 ^= x1;
			x0 = ~x0;
			x0 ^= x3;
			x3 |= x1;
			x3 ^= x1;
			x3 |= x0;
			x2 ^= x3;
		}


		private void sboxI0()
		{
			x2 = (x2 >>> 22) | (x2 << 10);
			x0 = (x0 >>> 5) | (x0 << 27);
			x3 = x1;
			x2 ^= x4;
			x3 <<= 7;
			x0 ^= x4;
			x2 ^= x3;
			x0 ^= x1;
			x4 = (x4 >>> 7) | (x4 << 25);
			x1 = (x1 >>> 1) | (x1 << 31);
			x1 ^= x0;
			x3 = x0 << 3;
			x4 ^= x3;
			x0 = (x0 >>> 13) | (x0 << 19);
			x1 ^= x2;
			x4 ^= x2;
			x2 = (x2 >>> 3) | (x2 << 29);
			x2 = ~x2;
			x3 = x1;
			x1 |= x0;
			x3 = ~x3;
			x1 ^= x2;
			x2 |= x3;
			x1 ^= x4;
			x0 ^= x3;
			x2 ^= x0;
			x0 &= x4;
			x3 ^= x0;
			x0 |= x1;
			x0 ^= x2;
			x4 ^= x3;
			x2 ^= x1;
			x4 ^= x0;
			x4 ^= x1;
			x2 &= x4;
			x3 ^= x2;
		}


		private void sboxI7()
		{
			x1 = (x1 >>> 22) | (x1 << 10);
			x0 = (x0 >>> 5) | (x0 << 27);
			x2 = x3;
			x1 ^= x4;
			x2 <<= 7;
			x0 ^= x4;
			x1 ^= x2;
			x0 ^= x3;
			x4 = (x4 >>> 7) | (x4 << 25);
			x3 = (x3 >>> 1) | (x3 << 31);
			x3 ^= x0;
			x2 = x0 << 3;
			x4 ^= x2;
			x0 = (x0 >>> 13) | (x0 << 19);
			x3 ^= x1;
			x4 ^= x1;
			x1 = (x1 >>> 3) | (x1 << 29);
			x2 = x1;
			x1 ^= x0;
			x0 &= x4;
			x1 = ~x1;
			x2 |= x4;
			x4 ^= x3;
			x3 |= x0;
			x0 ^= x1;
			x1 &= x2;
			x3 ^= x1;
			x1 ^= x0;
			x0 |= x1;
			x4 &= x2;
			x0 ^= x4;
			x2 ^= x3;
			x4 ^= x2;
			x2 |= x0;
			x4 ^= x1;
			x2 ^= x1;
		}

		// These S-Box functions are used in the key setup.

		/**
		 * S-Box 0.
		 */
		private void sbox0(int r0, int r1, int r2, int r3)
		{
			int r4 = r1 ^ r2;
			r3 ^= r0;
			r1 = r1 & r3 ^ r0;
			r0 = (r0 | r3) ^ r4;
			r4 ^= r3;
			r3 ^= r2;
			r2 = (r2 | r1) ^ r4;
			r4 = ~r4 | r1;
			r1 ^= r3 ^ r4;
			r3 |= r0;
			x0 = r1 ^ r3;
			x1 = r4 ^ r3;
			x2 = r2;
			x3 = r0;
		}


		/**
		 * S-Box 1.
		 */
		private void sbox1(int r0, int r1, int r2, int r3)
		{
			r0 = ~r0;
			int r4 = r0;
			r2 = ~r2;
			r0 &= r1;
			r2 ^= r0;
			r0 |= r3;
			r3 ^= r2;
			r1 ^= r0;
			r0 ^= r4;
			r4 |= r1;
			r1 ^= r3;
			r2 = (r2 | r0) & r4;
			r0 ^= r1;
			x0 = r2;
			x1 = r0 & r2 ^ r4;
			x2 = r3;
			x3 = r1 & r2 ^ r0;
		}


		/**
		 * S-Box 2.
		 */
		private void sbox2(int r0, int r1, int r2, int r3)
		{
			int r4 = r0;
			r0 = r0 & r2 ^ r3;
			r2 = r2 ^ r1 ^ r0;
			r3 = (r3 | r4) ^ r1;
			r4 ^= r2;
			r1 = r3;
			r3 = (r3 | r4) ^ r0;
			r0 &= r1;
			r4 ^= r0;
			x0 = r2;
			x1 = r3;
			x2 = r1 ^ r3 ^ r4;
			x3 = ~r4;
		}


		/**
		 * S-Box 3.
		 */
		private void sbox3(int r0, int r1, int r2, int r3)
		{
			int r4 = r0;
			r0 |= r3;
			r3 ^= r1;
			r1 &= r4;
			r4 = r4 ^ r2 | r1;
			r2 ^= r3;
			r3 = r3 & r0 ^ r4;
			r0 ^= r1;
			r4 = r4 & r0 ^ r2;
			r1 = (r1 ^ r3 | r0) ^ r2;
			r0 ^= r3;
			x0 = (r1 | r3) ^ r0;
			x1 = r1;
			x2 = r3;
			x3 = r4;
		}


		/**
		 * S-Box 4.
		 */
		private void sbox4(int r0, int r1, int r2, int r3)
		{
			r1 ^= r3;
			int r4 = r1;
			r3 = ~r3;
			r2 ^= r3;
			r3 ^= r0;
			r1 = r1 & r3 ^ r2;
			r4 ^= r3;
			r0 ^= r4;
			r2 = r2 & r4 ^ r0;
			r0 &= r1;
			r3 ^= r0;
			r4 = (r4 | r1) ^ r0;
			x0 = r1;
			x1 = r4 ^ (r2 & r3);
			x2 = ~((r0 | r3) ^ r2);
			x3 = r3;
		}


		/**
		 * S-Box 5.
		 */
		private void sbox5(int r0, int r1, int r2, int r3)
		{
			r0 ^= r1;
			r1 ^= r3;
			int r4 = r1;
			r3 = ~r3;
			r1 &= r0;
			r2 ^= r3;
			r1 ^= r2;
			r2 |= r4;
			r4 ^= r3;
			r3 = r3 & r1 ^ r0;
			r4 = r4 ^ r1 ^ r2;
			x0 = r1;
			x1 = r3;
			x2 = r0 & r3 ^ r4;
			x3 = ~(r2 ^ r0) ^ (r4 | r3);
		}


		/**
		 * S-Box 6.
		 */
		private void sbox6(int r0, int r1, int r2, int r3)
		{
			int r4 = r3;
			r2 = ~r2;
			r3 = r3 & r0 ^ r2;
			r0 ^= r4;
			r2 = (r2 | r4) ^ r0;
			r1 ^= r3;
			r0 |= r1;
			r2 ^= r1;
			r4 ^= r0;
			r0 = (r0 | r3) ^ r2;
			r4 = r4 ^ r3 ^ r0;
			x0 = r0;
			x1 = r1;
			x2 = r4;
			x3 = r2 & r4 ^ ~r3;
		}


		/**
		 * S-Box 7.
		 */
		private void sbox7(int r0, int r1, int r2, int r3)
		{
			int r4 = r1;
			r1 = (r1 | r2) ^ r3;
			r4 ^= r2;
			r2 ^= r1;
			r3 = (r3 | r4) & r0;
			r4 ^= r2;
			r3 ^= r1;
			r1 = (r1 | r4) ^ r0;
			r0 = (r0 | r4) ^ r2;
			r1 ^= r4;
			r2 ^= r1;
			x0 = r4 ^ (~r2 | r0);
			x1 = r3;
			x2 = r1 & r0 ^ r4;
			x3 = r0;
		}
	}
}
