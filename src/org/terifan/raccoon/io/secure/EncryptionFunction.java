package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.cryptography.Serpent;
import org.terifan.security.cryptography.Twofish;


public enum EncryptionFunction
{
	AES,
	Twofish,
	Serpent,
	AESTwofish,
	TwofishSerpent,
	SerpentAES,
	AESTwofishSerpent,
	TwofishAESSerpent,
	SerpentTwofishAES;


	BlockCipher[] newInstance()
	{
		switch (this)
		{
			case AES:
				return new BlockCipher[]{new AES()};
			case Twofish:
				return new BlockCipher[]{new Twofish()};
			case Serpent:
				return new BlockCipher[]{new Serpent()};
			case AESTwofish:
				return new BlockCipher[]{new AES(), new Twofish()};
			case TwofishSerpent:
				return new BlockCipher[]{new Twofish(), new Serpent()};
			case SerpentAES:
				return new BlockCipher[]{new Serpent(), new AES()};
			case AESTwofishSerpent:
				return new BlockCipher[]{new AES(), new Twofish(), new Serpent()};
			case TwofishAESSerpent:
				return new BlockCipher[]{new Twofish(), new AES(), new Serpent()};
			case SerpentTwofishAES:
				return new BlockCipher[]{new Serpent(), new Twofish(), new AES()};
		}

		throw new IllegalStateException();
	}


	BlockCipher newTweakInstance()
	{
		switch (this)
		{
			case AES:
			case AESTwofish:
			case AESTwofishSerpent:
				return new AES();
			case Twofish:
			case TwofishSerpent:
			case TwofishAESSerpent:
				return new Twofish();
			case Serpent:
			case SerpentAES:
			case SerpentTwofishAES:
				return new Serpent();
		}

		throw new IllegalStateException();
	}
}