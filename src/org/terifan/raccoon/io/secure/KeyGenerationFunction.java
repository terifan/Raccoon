package org.terifan.raccoon.io.secure;

import java.security.MessageDigest;
import org.terifan.security.messagedigest.SHA3;
import org.terifan.security.messagedigest.SHA512;
import org.terifan.security.messagedigest.Skein512;
import org.terifan.security.messagedigest.Whirlpool;


public enum KeyGenerationFunction
{
	SHA512,
	Skein512,
	Whirlpool,
	SHA3;


	MessageDigest newInstance()
	{
		switch (this)
		{
			case SHA3:
				return new SHA3();
			case SHA512:
				return new SHA512();
			case Skein512:
				return new Skein512();
			case Whirlpool:
				return new Whirlpool();
		}

		throw new IllegalStateException();
	}
}