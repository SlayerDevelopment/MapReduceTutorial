package com.slayer.portafolio.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TransactionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable UNO = new IntWritable(1);

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String dataSet = value.toString();
		String[] dataFrame = dataSet.split(",");
		output.collect(new Text(dataFrame[5]), UNO);
	}

}
