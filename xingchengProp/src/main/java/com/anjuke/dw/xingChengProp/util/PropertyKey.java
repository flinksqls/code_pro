package com.anjuke.dw.xingChengProp.util;


import scala.tools.nsc.doc.model.Val;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertyKey {
     String value() default "";
}
