package com.slayer.portafolio.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TransactionCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
			Reporter reporter) throws IOException {
		int frecuenciaPaises = 0;
		while (values.hasNext()) {
			frecuenciaPaises += values.next().get();
		}
		output.collect(key, new IntWritable(frecuenciaPaises));
	}

}
