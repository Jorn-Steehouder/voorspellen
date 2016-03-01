package main.java.nl.hu.hadoop.wordcount;

import java.util.Set;
import java.util.TreeSet;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {
	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(WordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public int getDivisorSum(int n) {
		int root = (int) Math.sqrt(n);
		int sum = 1;
		int addFornumber = 1;
		int beginFornumber = 2;

		if (n % 2 != 0) { //check of hij oneven is
			addFornumber = 2;
			beginFornumber = 3;
		}

		for (int i = beginFornumber; i <= root; i += addFornumber) {
			if (n % i == 0) {
				sum += i;
				int d = n / i;
				if (d != i)
					sum += d;
			}
		}
		return sum;
	}

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		int max = Integer.parseInt(value.toString());
		//int max = 1000000;
		//Bovenstaand comment gebruiken voor als er niks staat in de input file

		for (int i = 1; i <= max; i++) {
			int a = getDivisorSum(i);
			int b = getDivisorSum(a);
			if ((i == b && a != b) && i > a) {
				context.write(new Text(Integer.toString(i)), new IntWritable(a));
			}
		}
	}
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int hoihoi = 0;

		for (IntWritable i : values) {
			hoihoi = i.get();
		}
		context.write(key, new IntWritable(hoihoi));
	}
}
