package org.hilib.sample.mapper;

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io._
import org.hilib.sample.io.TemperatureKey

object NOAADataFormat {
  val YEAR_BEGIN_INDEX = 15
  val YEAR_END_INDEX = 19
  val ID_BEGIN_INDEX = 4
  val ID_END_INDEX = 15
  val DATETIME_BEGIN_INDEX = 15
  val DATETIME_END_INDEX = 27
  val TEMP_BEGIN_INDEX = 87
  val TEMP_END_INDEX = 92
}

class TemperatureMapper extends Mapper[LongWritable, Text, TemperatureKey, IntWritable] {
  type Context = Mapper[LongWritable, Text, TemperatureKey, IntWritable]#Context
  override def map(key: LongWritable, value: Text, context: Context) {
    val line = value.toString()
    val year = line.substring(NOAADataFormat.YEAR_BEGIN_INDEX,
      NOAADataFormat.YEAR_END_INDEX)
    val id = line.substring(NOAADataFormat.ID_BEGIN_INDEX,
      NOAADataFormat.ID_END_INDEX)
    val datetime = line.substring(NOAADataFormat.DATETIME_BEGIN_INDEX,
      NOAADataFormat.DATETIME_END_INDEX)
    val temp = line.substring(NOAADataFormat.TEMP_BEGIN_INDEX,
      NOAADataFormat.TEMP_END_INDEX)
    if (!(temp eq "+9999")) {
      val airTemperature = Integer.parseInt(temp)
      context.write(new TemperatureKey(id, datetime),
        new IntWritable(airTemperature))
    }
  }
}
