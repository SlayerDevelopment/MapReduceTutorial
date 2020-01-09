package com.slayer.portafolio.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TransactionCountryDriver {

	public static void main(String[] args) {
		JobClient jobClient = new JobClient();
		JobConf jobConf = new JobConf(TransactionCountryDriver.class);
		jobConf.setJobName("contandoPaises");
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		jobConf.setMapperClass(TransactionMapper.class);
		jobConf.setReducerClass(TransactionCountryReducer.class);
		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
		jobClient.setConf(jobConf);
		try {
			JobClient.runJob(jobConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
