package dev.mruss.sortstrings

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool

class SetDifference : Configured(), Tool {
    override fun run(args: Array<out String>): Int {
        val job = Job.getInstance(conf, "setDifference")
        job.setJarByClass(this::class.java)

        // set up the map-reduce pipeline input and output file locations
        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[2]))
        job.mapperClass = Map::class.java
        job.reducerClass = Reduce::class.java

        // setup the map key value types
        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = Text::class.java
        job.outputKeyClass = NullWritable::class.java
        job.outputValueClass = Text::class.java

        val success = job.waitForCompletion(true)

        return if (success) 0 else 1
    }

    class Map : Mapper<LongWritable, Text, Text, Text>() {
        override fun map(offset: LongWritable, line: Text, context: Context) {
            val lineStr = line.toString()
            val splitLine = lineStr
                .split(',')

            // this line writes to the map reduce context (?) to provide
            // the intermediate outputs for the reducer
            // emit the left and right elements with appropriate values
            context.write(Text(splitLine[0]), Text("L"))
            context.write(Text(splitLine[1]), Text("R"))
        }
    }

    class Reduce : Reducer<Text, Text, NullWritable, Text>() {
        override fun reduce(key: Text, values: MutableIterable<Text>, context: Context) {
            // the key is the string
            // the value is whether in right or left
            // check if the value is in the right side, but never appeared in the left
            if (values.contains(Text("R")) &&
                !values.contains(Text("L"))) {
                context.write(NullWritable.get(), key)
            }
        }
    }
}