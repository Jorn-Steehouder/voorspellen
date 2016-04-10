package main.java.nl.hu.hadoop.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		getItDone();

	}

	private static void getItDone() throws IOException {
		FileReader fr = new FileReader("/home/jorns/Downloads/hadoop-2.7.2/output/part-r-00000");
		BufferedReader br = new BufferedReader(fr);

		String inputje = "/_/he hell/_/ wor/_/d"; // symbool voor letter = /_/
		FileWriter fw = new FileWriter("/home/jorns/Downloads/output.txt");

		HashMap<String, HashMap<String, Integer>> before4Current = new HashMap<String, HashMap<String, Integer>>();
		HashMap<String, HashMap<String, Integer>> after4Current = new HashMap<String, HashMap<String, Integer>>();

		String currentLetter = "";
		String line;
		while ((line = br.readLine()) != null) {
			HashMap<String, Integer> beforeLetterCounts = new HashMap<String, Integer>();
			HashMap<String, Integer> afterLetterCounts = new HashMap<String, Integer>();
			currentLetter = line.split("/beforecount/")[0];
			currentLetter = currentLetter.replaceAll("\\s+", "");
			if (currentLetter.equals("")) {
				currentLetter = " ";
			}
			String[] beforeCounters = line.split("/beforecount/")[1].split("/,/");
			for (String letterCount : beforeCounters) {
				String letter = letterCount.split("/=/")[0];
				int count = Integer.parseInt(letterCount.split("/=/")[1]);
				beforeLetterCounts.put(letter, count);
			}
			before4Current.put(currentLetter, beforeLetterCounts);

			String secondLine = br.readLine();
			String[] afterCounters = secondLine.split("/aftercount/")[1].split("/,/");
			for (String letterCount : afterCounters) {
				String letter = letterCount.split("/=/")[0];
				int count = Integer.parseInt(letterCount.split("/=/")[1]);
				afterLetterCounts.put(letter, count);

			}
			after4Current.put(currentLetter, afterLetterCounts);

		}

		String outputje = "";
		String[] splittedInput = inputje.split("\\s+");
		for (String word : splittedInput) {
			word = " " + word + " ";

			HashMap<String, Integer> singleHashAfter = after4Current
					.get(word.split("/_/")[0].substring(word.split("/_/")[0].length() - 1));
			HashMap<String, Integer> singleHashBefore = before4Current
					.get(Character.toString(word.split("/_/")[1].toCharArray()[0]));

			int sum = 0;
			String theKey = "";
			for (String key : singleHashAfter.keySet()) {
				if (singleHashBefore.containsKey(key)) {
					if ((singleHashAfter.get(key) + singleHashBefore.get(key)) > sum) {
						theKey = key;
						sum = singleHashAfter.get(key) + singleHashBefore.get(key);
					}
				}
			}

			word = word.split("/_/")[0] + theKey + word.split("/_/")[1];
			outputje += word;

		}
		fw.write(outputje);
		fr.close();
		fw.close();

	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] allChars = value.toString().split("");
		int letterCount = allChars.length - 1;

		for (int i = 1; i < letterCount; i++) {
			String curr = allChars[i];
			String prev = allChars[(i - 1)];
			String next = allChars[(i + 1)];

			String text = prev + "/hellue/" + next;
			context.write(new Text(curr), new Text(text));

		}
	}
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> allLettersBeforeKey = new ArrayList<String>();
		ArrayList<String> allLettersAfterKey = new ArrayList<String>();
		String beforeReadable = "/beforecount/", afterReadable = "/aftercount/";

		for (Text val : values) {
			String[] splitted = val.toString().split("/hellue/");
			allLettersBeforeKey.add(splitted[0]);
			allLettersAfterKey.add(splitted[1]);
		}

		Set<String> letterBeforeKey = new HashSet<String>(allLettersBeforeKey);
		Set<String> letterAfterKey = new HashSet<String>(allLettersAfterKey);

		for (String letter : letterBeforeKey) {
			int count = 0;
			for (String forLetter : allLettersBeforeKey) {
				if (letter.equals(forLetter)) {
					count++;
				}
			}
			if (count != 0) {
				beforeReadable += letter + "/=/" + count + "/,/";
			}
		}

		for (String letter : letterAfterKey) {
			int count = 0;
			for (String forLetter : allLettersAfterKey) {
				if (letter.equals(forLetter)) {
					count++;
				}
			}
			if (count != 0) {
				afterReadable += letter + "/=/" + count + "/,/";
			}
		}
		String s = beforeReadable + "\n\t" + afterReadable;
		context.write(key, new Text(s));
	}
}
