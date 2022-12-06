package org.terifan.bundle;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import static org.terifan.bundle.VarType.values;


enum VarType
{
	TERMINATOR(0),
	BUNDLE(1),
	ARRAY(2),
	INT(3,
		(aOutput, aValue) -> aOutput.writeVarint((Integer)aValue),
		aInput -> (int)aInput.readVarint()
	),
	DOUBLE(4,
		(aOutput, aValue) -> aOutput.writeVarint(Long.reverseBytes(Double.doubleToLongBits((Double)aValue))),
		aInput -> Double.longBitsToDouble(Long.reverseBytes(aInput.readVarint()))
	),
	BOOLEAN(5,
		(aOutput, aValue) -> aOutput.writeVarint((Boolean)aValue ? 1 : 0),
		aInput -> aInput.readVarint() == 1
	),
	STRING(6,
		(aOutput, aValue) -> aOutput.writeString(aValue.toString()),
		aInput -> aInput.readString()
	),
	NULL(7,
		(aOutput, aValue) -> {},
		aInput -> null
	),
	BYTE(8,
		(aOutput, aValue) -> aOutput.writeVarint(0xff & (Byte)aValue),
		aInput -> (byte)aInput.readVarint()
	),
	SHORT(9,
		(aOutput, aValue) -> aOutput.writeVarint((Short)aValue),
		aInput -> (short)aInput.readVarint()
	),
	LONG(10,
		(aOutput, aValue) -> aOutput.writeVarint((Long)aValue),
		aInput -> aInput.readVarint()
	),
	FLOAT(11,
		(aOutput, aValue) -> aOutput.writeVarint(Float.floatToIntBits((Float)aValue)),
		aInput -> Float.intBitsToFloat((int)aInput.readVarint())
	),
	DATE(12,
		(aOutput, aValue) -> aOutput.writeVarint(((Date)aValue).getTime()),
		aInput -> new Date(aInput.readVarint())
	),
	BINARY(13,
		(aOutput, aValue) -> aOutput.writeBuffer((byte[])aValue),
		aInput -> aInput.readBuffer()
	),
	UUID(14,
		(aOutput, aValue) -> aOutput.writeVarint(((UUID)aValue).getMostSignificantBits()).writeVarint(((UUID)aValue).getLeastSignificantBits()),
		aInput -> new java.util.UUID(aInput.readVarint(), aInput.readVarint())
	);

	public final int code;
	public Encoder encoder;
	public Decoder decoder;


	private VarType(int aCode)
	{
		code = aCode;
	}


	private VarType(int aCode, Encoder aEncoder, Decoder aDecoder)
	{
		code = aCode;
		encoder = aEncoder;
		decoder = aDecoder;
	}


	public static VarType get(int aCode)
	{
		VarType type = values()[aCode];
		assert type.ordinal() == aCode;
		return type;
	}


	static VarType identify(Object aValue)
	{
		if (aValue == null)
		{
			return NULL;
		}

		Class<? extends Object> cls = aValue.getClass();

		if (Document.class == cls) return BUNDLE;
		if (Array.class == cls) return ARRAY;
		if (String.class == cls) return STRING;
		if (Integer.class == cls) return INT;
		if (Boolean.class == cls) return BOOLEAN;
		if (Double.class == cls) return DOUBLE;
		if (Long.class == cls) return LONG;
		if (Float.class == cls) return FLOAT;
		if (Byte.class == cls) return BYTE;
		if (Short.class == cls) return SHORT;
		if (Date.class == cls) return DATE;
		if (UUID.class == cls) return UUID;
		if (byte[].class == cls) return BINARY;

		throw new IllegalArgumentException("Failed to marshal an unsupported value of type " + cls.getCanonicalName());
	}


	@FunctionalInterface
	static interface Encoder
	{
		void encode(VarOutputStream aOutput, Object aValue) throws IOException;
	}


	@FunctionalInterface
	static interface Decoder
	{
		Object decode(VarInputStream aInput) throws IOException;
	}


	private static Object readEnum(String aEnumFullName) throws IOException
	{
		try
		{
			int i = aEnumFullName.lastIndexOf('.');
			String name = aEnumFullName.substring(i + 1);
			Class cls = Class.forName(aEnumFullName.substring(0, i));
			for (Object o : cls.getEnumConstants())
			{
				if (((Enum)o).name().equals(name))
				{
					return o;
				}
			}
			throw new IOException("No enum constant for name: " + name);
		}
		catch (ClassNotFoundException e)
		{
			throw new IOException(e);
		}
	}
}
