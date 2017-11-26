package org.hilib.sample.reducer

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.io._
import collection.JavaConversions.iterableAsScalaIterable
import org.hilib.sample.io.TemperatureKey

class HBaseImportReducer extends Reducer[TemperatureKey, IntWritable, Text, IntWritable] {
  type Context = Reducer[TemperatureKey, IntWritable, Text, IntWritable]#Context

  var mos: MultipleOutputs[Text, IntWritable] = null
  var userName: String = ""

  override def setup(context: Context) = {
    mos = new MultipleOutputs(context)
    val name = context.getConfiguration().get("import.user.name")
    userName = if (name == null) "defaul" else name
  }
  override def reduce(key: TemperatureKey, values: java.lang.Iterable[IntWritable], context: Context) {
    var maxValue = Integer.MIN_VALUE
    for (value: IntWritable <- values) {
      maxValue = Math.max(maxValue, value.get())
    }
    mos.write("text", new Text(key._id + ":" + key._timestamp),
      new IntWritable(maxValue), userName + key.getDate())
//    context.write(new Text(key._id + ":" + key._timestamp),
//      new IntWritable(maxValue))
  }
  override def cleanup(context: Context) = {
    mos.close()
  }
}
