package com.aofeng.label.utils

import java.lang.reflect.Modifier

object CopyUtil {

  def copy[T](o: T, vals: (String, Any)*): T = {
    if (vals.isEmpty) return o
    val copier = new Copier(o.getClass)
    val maps = vals.map(kv => {
      kv._1 -> kv._2
    }).toMap
    copier(o, maps)
  }

  def copy[T](o: T, vals: Map[String, Any]): T = {
    if (vals.isEmpty) return o
    val copier = new Copier(o.getClass)
    copier(o, vals)
  }

  private class Copier(cls: Class[_]) {
    private val ctor = cls.getConstructors.apply(0)
    private val getters = cls.getDeclaredFields
      .filter {
        f =>
          val m = f.getModifiers
          Modifier.isPrivate(m) && Modifier.isFinal(m) && !Modifier.isStatic(m)
      }
      .take(ctor.getParameterTypes.length)
      .map(f => cls.getMethod(f.getName))

    def apply[T](o: T, vals: Map[String, Any]): T = {
      val byIx = vals.map {
        case (name, value) =>
          val ix = getters.indexWhere(_.getName == name)
          if (ix < 0) throw new IllegalArgumentException("Unknown field: " + name)
          (ix, value.asInstanceOf[Object])
      }.toMap

      val args = getters.indices.map {
        i =>
          byIx.getOrElse(i, getters(i).invoke(o))
      }
      ctor.newInstance(args: _*).asInstanceOf[T]
    }

  }

}
