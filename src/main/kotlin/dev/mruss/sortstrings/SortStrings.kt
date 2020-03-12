package dev.mruss.sortstrings

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.Tool

class SortStrings : Configured(), Tool {
    companion object {
        const val pairCountOutputName = "pairCount"
    }
    enum class SORTSTR_COUNTER {
        UNIQUE_PAIRS
    }
    override fun run(args: Array<out String>): Int {
        val job = Job.getInstance(conf, "sortStrings")
        job.setJarByClass(this::class.java)
        // use multiple outputs to name the output
        MultipleOutputs.addNamedOutput(
            job,
            pairCountOutputName,
            TextOutputFormat::class.java,
            Text::class.java,
            IntWritable::class.java
        )

        // set up the map-reduce pipeline input and output file locations
        FileInputFormat.addInputPath(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))
        job.mapperClass = Map::class.java
        job.reducerClass = Reduce::class.java

        // setup the output reducer key value types
        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        val success = job.waitForCompletion(true)

        // grab the counter value
        val pairCounter = job.counters.findCounter(SORTSTR_COUNTER.UNIQUE_PAIRS)
        println("\n\nUNIQUE PAIRS: ${pairCounter.value}\n")

        return if (success) 0 else 1
    }

    class Map : Mapper<LongWritable, Text, Text, IntWritable>() {
        override fun map(offset: LongWritable, line: Text, context: Context) {
            val lineStr = line.toString()
            val sortedLine = lineStr
                .split(',')
                .sorted()
                .joinToString(",")
            // write the sorted pair
            // this line writes to the map reduce context to provide
            // the intermediate outputs for the reducer
            // output the sorted key and 1
            context.write(Text(sortedLine), IntWritable(1))
        }
    }

    class Reduce : Reducer<Text, IntWritable, Text, IntWritable>() {
        private var multipleOutputs: MultipleOutputs<Text,IntWritable>? = null
        override fun setup(context: Context) {
            super.setup(context)
            multipleOutputs = MultipleOutputs(context)
        }
        override fun reduce(key: Text, values: MutableIterable<IntWritable>, context: Context) {
            // same as word count
            // the key is the sorted pair
            // sum the values to count each ordered pair
            var sum = 0
            for (count in values) {
                sum += count.get()
            }
            // use multiple outputs to name the output
            multipleOutputs?.write(pairCountOutputName, key, IntWritable(sum))

            // add one to unique pair counter
            context.getCounter(SORTSTR_COUNTER.UNIQUE_PAIRS).increment(1)
        }

        override fun cleanup(context: Context?) {
            super.cleanup(context)
            multipleOutputs?.close()
        }
    }
}