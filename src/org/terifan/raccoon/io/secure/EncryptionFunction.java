package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.AES;
import org.terifan.security.cryptography.BlockCipher;
import org.terifan.security.cryptography.Kuznechik;
import org.terifan.security.cryptography.Serpent;
import org.terifan.security.cryptography.Twofish;


public enum EncryptionFunction
{
	AES,
	TWOFISH,
	SERPENT,
	KUZNECHIK,
	AES_TWOFISH,
	TWOFISH_SERPENT,
	SERPENT_AES,
	KUZNECHIK_AES,
	AES_TWOFISH_SERPENT,
	TWOFISH_AES_SERPENT,
	SERPENT_TWOFISH_AES,
	KUZNECHIK_TWOFISH_AES;


	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		for (String s : name().split("_"))
		{
			if (sb.length() > 0)
			{
				sb.append("-");
			}
			sb.append(s.substring(0,1)+s.substring(1).toLowerCase());
		}
		return sb.toString().replace("Aes","AES");
	}


	BlockCipher[] newInstance()
	{
		switch (this)
		{
			case AES:
				return new BlockCipher[]{new AES()};
			case TWOFISH:
				return new BlockCipher[]{new Twofish()};
			case SERPENT:
				return new BlockCipher[]{new Serpent()};
			case KUZNECHIK:
				return new BlockCipher[]{new Kuznechik()};
			case AES_TWOFISH:
				return new BlockCipher[]{new AES(), new Twofish()};
			case TWOFISH_SERPENT:
				return new BlockCipher[]{new Twofish(), new Serpent()};
			case SERPENT_AES:
				return new BlockCipher[]{new Serpent(), new AES()};
			case KUZNECHIK_AES:
				return new BlockCipher[]{new Kuznechik(), new AES()};
			case AES_TWOFISH_SERPENT:
				return new BlockCipher[]{new AES(), new Twofish(), new Serpent()};
			case TWOFISH_AES_SERPENT:
				return new BlockCipher[]{new Twofish(), new AES(), new Serpent()};
			case SERPENT_TWOFISH_AES:
				return new BlockCipher[]{new Serpent(), new Twofish(), new AES()};
			case KUZNECHIK_TWOFISH_AES:
				return new BlockCipher[]{new Kuznechik(), new Twofish(), new AES()};
		}

		throw new IllegalStateException();
	}


	BlockCipher newTweakInstance()
	{
		switch (this)
		{
			case AES:
			case AES_TWOFISH:
			case AES_TWOFISH_SERPENT:
				return new AES();
			case TWOFISH:
			case TWOFISH_SERPENT:
			case TWOFISH_AES_SERPENT:
				return new Twofish();
			case SERPENT:
			case SERPENT_AES:
			case SERPENT_TWOFISH_AES:
				return new Serpent();
			case KUZNECHIK:
				return new Kuznechik();
		}

		throw new IllegalStateException();
	}
}