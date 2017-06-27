package org.terifan.raccoon.io.secure;

import org.terifan.security.cryptography.CBCCipherMode;
import org.terifan.security.cryptography.CipherMode;
import org.terifan.security.cryptography.PCBCCipherMode;
import org.terifan.security.cryptography.XTSCipherMode;


public enum CipherModeFunction
{
	XTS,
	CBC,
	PCBC;


	CipherMode newInstance()
	{
		switch (this)
		{
			case CBC:
				return new CBCCipherMode();
			case PCBC:
				return new PCBCCipherMode();
			case XTS:
				return new XTSCipherMode();
		}

		throw new IllegalStateException();
	}
}