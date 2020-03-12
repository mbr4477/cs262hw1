package dev.mruss.sortstrings

import org.apache.hadoop.util.ToolRunner
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val sortResult = ToolRunner.run(SortStrings(), args)
    val diffResult = ToolRunner.run(SetDifference(), args)
    exitProcess(0)
}