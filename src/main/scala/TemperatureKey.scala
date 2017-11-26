package org.hilib.sample.io

import java.io.DataOutput
import java.io.DataInput
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.Text

class TemperatureKey(id: String = "", timestamp: String = "") extends WritableComparable[TemperatureKey] {
  var _id = id
  var _timestamp = timestamp

  def this() = this("", "")

  def getDate(): String = this._timestamp.substring(0, 8)

  override def write(out: DataOutput) {
    Text.writeString(out, _id)
    Text.writeString(out, _timestamp)
  }

  override def readFields(in: DataInput) = {
    this._id = Text.readString(in)
    this._timestamp = Text.readString(in)
  }

  override def compareTo(that: TemperatureKey): Int = {
    val cmp = this._id.compareTo(that._id)
    if (cmp != 0) {
      return cmp
    }
    return this._timestamp.compareTo(that._timestamp)
  }

  override def equals(obj: Any): Boolean = {
    if (obj.isInstanceOf[TemperatureKey]) {
      val that = obj.asInstanceOf[TemperatureKey]
      if (this._id.equals(that._id)) {
        return false
      }
      return this._timestamp.equals(that._timestamp)
    }
    return false
  }

  override def hashCode(): Int = {
    val key = this._id + ":" + this.getDate()
    return key.hashCode()
  }
}
