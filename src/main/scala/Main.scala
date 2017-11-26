package org.hilib.sample;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.RunningJob
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.{MultipleOutputs, TextOutputFormat, LazyOutputFormat}
import org.apache.hadoop.io._
import org.apache.hadoop.fs.Path
import org.hilib.sample.io.TemperatureKey
import org.hilib.sample.mapper.TemperatureMapper
import org.hilib.sample.reducer.MaxTemperatureReducer
import org.hilib.sample.reducer.HBaseImportReducer

object Main {
  def createJob(args: Array[String]): Job = {
    val job = Job.getInstance()
    job.setJarByClass(getClass())
    job.setJobName("myjob")

    FileInputFormat.addInputPaths(job, args(0))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    job.getConfiguration().set("import.user.name", args(2))

    job.setMapperClass(classOf[TemperatureMapper])
    job.setMapOutputKeyClass(classOf[TemperatureKey])
//    job.setReducerClass(classOf[MaxTemperatureReducer])
    job.setReducerClass(classOf[HBaseImportReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setNumReduceTasks(5)

    LazyOutputFormat.setOutputFormatClass(job, classOf[TextOutputFormat[Text, IntWritable]])
    MultipleOutputs.addNamedOutput(job, "text", classOf[TextOutputFormat[Text, IntWritable]],
      classOf[Text], classOf[IntWritable])

    return job
  }

  def main(args: Array[String]) {
    val job = Main.createJob(args)
    job.waitForCompletion(true)
  }
}
