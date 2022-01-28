package com.aofeng.label.reader

trait Reader[IN, OUT] extends Serializable {

  def read(in: IN): OUT
}
