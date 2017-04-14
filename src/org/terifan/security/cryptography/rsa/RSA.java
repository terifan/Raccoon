package org.terifan.security.cryptography.rsa;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.Cipher;


public class RSA
{
	public static KeyPair generateKeyPair(int aLength) throws NoSuchAlgorithmException
	{
		KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
		generator.initialize(aLength);
		return generator.generateKeyPair();
	}


	public static RSAPrivateKey decodePKCS8EncodedPrivateKey(String aKey) throws IOException, GeneralSecurityException
	{
		System.out.println(aKey);

		aKey = aKey.replace("-----BEGIN PRIVATE KEY-----", "");
		aKey = aKey.replace("-----END PRIVATE KEY-----", "");
		aKey = aKey.replace("\r", "").replace("\n", "").trim();

		byte[] encoded = decodeBase64(aKey);
		KeyFactory kf = KeyFactory.getInstance("RSA");
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
		RSAPrivateKey privKey = (RSAPrivateKey)kf.generatePrivate(keySpec);
		return privKey;
	}


	public static PrivateKey decodePKCS1EncodedPrivateKey(String aKey) throws IOException, GeneralSecurityException
	{
		aKey = aKey.replace("-----BEGIN RSA PRIVATE KEY-----", "");
		aKey = aKey.replace("-----END RSA PRIVATE KEY-----", "");
		aKey = aKey.replace("\r", "").replace("\n", "").trim();

		byte[] encoded = decodeBase64(aKey);

		DERReader reader = new DERReader(encoded);

		Asn1Object sequence = reader.read();
		if (sequence.getType() != DERConstants.SEQUENCE)
		{
			throw new IOException("Invalid DER: not a sequence");
		}

		// Parse inside the sequence
		reader = sequence.getParser();

		reader.read(); // Skip version
		BigInteger modulus = reader.read().getInteger();
		BigInteger publicExp = reader.read().getInteger();
		BigInteger privateExp = reader.read().getInteger();
		BigInteger prime1 = reader.read().getInteger();
		BigInteger prime2 = reader.read().getInteger();
		BigInteger exp1 = reader.read().getInteger();
		BigInteger exp2 = reader.read().getInteger();
		BigInteger crtCoef = reader.read().getInteger();

		RSAPrivateCrtKeySpec keySpec = new RSAPrivateCrtKeySpec(modulus, publicExp, privateExp, prime1, prime2, exp1, exp2, crtCoef);

		KeyFactory kf = KeyFactory.getInstance("RSA");
        PrivateKey privKey = kf.generatePrivate(keySpec);

		return privKey;
	}


	public static String encodePKCS8EncodedPrivateKey(PrivateKey aPrivateKey) throws IOException, GeneralSecurityException
	{
		assertArgument(aPrivateKey.getAlgorithm(), "RSA", "Bad algorithm");
		assertArgument(aPrivateKey.getFormat(), "PKCS#8", "Bad format");

		return "-----BEGIN PRIVATE KEY-----\n"
			+ encodeBase64(aPrivateKey.getEncoded())
			+ "-----END PRIVATE KEY-----";
	}


	private static void assertArgument(String aActual, String aExpected, String aMessage) throws IllegalArgumentException
	{
		if (!aExpected.equals(aActual))
		{
			throw new IllegalArgumentException(aMessage + ": " + aActual + ", expected: " + aExpected);
		}
	}


	public static String encodePKCS1EncodedPrivateKey(PrivateKey aPrivateKey) throws IOException, GeneralSecurityException
	{
		assertArgument(aPrivateKey.getAlgorithm(), "RSA", "Bad algorithm");
		assertArgument(aPrivateKey.getFormat(), "PKCS#8", "Bad format");

		DERWriter seq = new DERWriter();

		RSAPrivateCrtKey keySpec = (RSAPrivateCrtKey)aPrivateKey;
		seq.write(0); // version
		seq.write(keySpec.getModulus());
		seq.write(keySpec.getPublicExponent());
		seq.write(keySpec.getPrivateExponent());
		seq.write(keySpec.getPrimeP());
		seq.write(keySpec.getPrimeQ());
		seq.write(keySpec.getPrimeExponentP());
		seq.write(keySpec.getPrimeExponentQ());
		seq.write(keySpec.getCrtCoefficient());

		DERWriter writer = new DERWriter();
		writer.write(seq);

		return "-----BEGIN RSA PRIVATE KEY-----\n"
			+ encodeBase64(writer.toByteArray())
			+ "-----END RSA PRIVATE KEY-----";
	}


	public static PublicKey decodePKCS8EncodedPublicKey(String key) throws IOException, GeneralSecurityException
	{
		String publicKeyPEM = key;
		publicKeyPEM = publicKeyPEM.replace("-----BEGIN PUBLIC KEY-----", "");
		publicKeyPEM = publicKeyPEM.replace("-----END PUBLIC KEY-----", "");
		publicKeyPEM = publicKeyPEM.trim();

		byte[] encoded = decodeBase64(publicKeyPEM);
		KeyFactory kf = KeyFactory.getInstance("RSA");
		PublicKey pubKey = (PublicKey)kf.generatePublic(new PKCS8EncodedKeySpec(encoded));
		return pubKey;
	}


	public static RSAPublicKey decodeX509EncodedPublicKey(String key) throws IOException, GeneralSecurityException
	{
		String publicKeyPEM = key;
		publicKeyPEM = publicKeyPEM.replace("-----BEGIN PUBLIC KEY-----", "");
		publicKeyPEM = publicKeyPEM.replace("-----END PUBLIC KEY-----", "");
		publicKeyPEM = publicKeyPEM.trim();

		byte[] encoded = decodeBase64(publicKeyPEM);
		X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(encoded);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return (RSAPublicKey)keyFactory.generatePublic(pubKeySpec);
	}


	public static String encodeX509EncodedPublicKey(PublicKey aPublicKey)
	{
		assertArgument(aPublicKey.getAlgorithm(), "RSA", "Bad algorithm");
		assertArgument(aPublicKey.getFormat(), "X.509", "Bad format");

		return "-----BEGIN PUBLIC KEY-----\n"
			+ encodeBase64(aPublicKey.getEncoded())
			+ "-----END PUBLIC KEY-----";
	}


	public static String sign(PrivateKey privateKey, String message) throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, UnsupportedEncodingException, InvalidKeyException
	{
		Signature sign = Signature.getInstance("SHA1withRSA");
		sign.initSign(privateKey);
		sign.update(message.getBytes("UTF-8"));
		return encodeBase64(sign.sign());
	}


	public static boolean verify(PublicKey publicKey, String message, String signature) throws SignatureException, NoSuchAlgorithmException, UnsupportedEncodingException, InvalidKeyException
	{
		Signature sign = Signature.getInstance("SHA1withRSA");
		sign.initVerify(publicKey);
		sign.update(message.getBytes("UTF-8"));
		return sign.verify(decodeBase64(signature));
	}


	public static String encrypt(String aPlainText, PublicKey publicKey) throws IOException, GeneralSecurityException
	{
		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.ENCRYPT_MODE, publicKey);
		return encodeBase64(cipher.doFinal(aPlainText.getBytes("UTF-8")));
	}


	public static String decrypt(String aCipherText, PrivateKey privateKey) throws IOException, GeneralSecurityException
	{
		Cipher cipher = Cipher.getInstance("RSA");
		cipher.init(Cipher.DECRYPT_MODE, privateKey);
		return new String(cipher.doFinal(decodeBase64(aCipherText)), "UTF-8");
	}


	private static String encodeBase64(byte[] aMessage)
	{
		String s = Base64.getEncoder().encodeToString(aMessage);

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i+=80)
		{
			sb.append(s.substring(i, Math.min(s.length(), i + 80))).append("\n");
		}
		return sb.toString();
	}


	private static byte[] decodeBase64(String aMessage)
	{
		try
		{
			return Base64.getDecoder().decode(aMessage.replace("\r", "").replace("\n", "").trim().getBytes("utf-8"));
		}
		catch (UnsupportedEncodingException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
