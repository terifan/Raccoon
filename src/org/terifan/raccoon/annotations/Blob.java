package org.terifan.raccoon.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target(value = {ElementType.METHOD, ElementType.FIELD}) @Retention(value = RetentionPolicy.RUNTIME)
public @interface Blob
{
	String name() default "";
}
