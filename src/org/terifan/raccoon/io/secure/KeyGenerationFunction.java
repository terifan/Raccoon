package org.terifan.raccoon.io.secure;

import java.security.MessageDigest;
import org.terifan.security.messagedigest.SHA3;
import org.terifan.security.messagedigest.SHA512;
import org.terifan.security.messagedigest.Skein512;
import org.terifan.security.messagedigest.Whirlpool;


public enum KeyGenerationFunction
{
	SHA512,
	SKEIN512,
	SHA3;


	MessageDigest newInstance()
	{
		switch (this)
		{
			case SHA512:
				return new SHA512();
			case SKEIN512:
				return new Skein512();
			case SHA3:
				return new SHA3(512);
		}

		throw new IllegalStateException();
	}
}