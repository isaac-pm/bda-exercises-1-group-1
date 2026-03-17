import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
	=========================================================
				HADOOP WORD COUNT DATA FLOW
	=========================================================

	[INPUT] "Hello world Hello"
		|
		v 
	+-------------------------------------------------------+
	| 1. MAP PHASE                                          |
	| Action: Tokenize text, emit count of 1 per word       |
	| Output: <"Hello", 1>, <"world", 1>, <"Hello", 1>      |
	+-------------------------------------------------------+
		|
		v 
	+-------------------------------------------------------+
	| 2. COMBINE & SHUFFLE PHASE                            |
	| Action: Local sum (Combiner) + Network Group/Sort     |
	| Output: <"Hello", [1, 1]>, <"world", [1]>             |
	+-------------------------------------------------------+
		|
		v
	+-------------------------------------------------------+
	| 3. REDUCE PHASE                                       |
	| Action: Sum the grouped lists of integers             |
	| Output: <"Hello", 2>, <"world", 1>                    |
	+-------------------------------------------------------+
		|
		v 
	[OUTPUT]
	Hello   2
	world   1
*/

public class HadoopWordCount extends Configured implements Tool {

	private static final Pattern SPLIT_PATTERN = Pattern.compile("[^a-z0-9.-]+");
	private static final Pattern WORD_PATTERN = Pattern.compile("[a-z-]{6,24}");
	private static final Pattern NUMBER_PATTERN = Pattern.compile("-?[0-9.]{4,16}");

	private static List<String> extractValidTokens(String line) {
		String lowerLine = line.toLowerCase(Locale.ROOT);
		String[] rawTokens = SPLIT_PATTERN.split(lowerLine);
		List<String> validTokens = new ArrayList<String>();

		for (String rawToken : rawTokens) {
			if (rawToken.isEmpty()) {
				continue;
			}

			if (WORD_PATTERN.matcher(rawToken).matches() || NUMBER_PATTERN.matcher(rawToken).matches()) {
				validTokens.add(rawToken);
			}
		}

		return validTokens;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = extractValidTokens(value.toString());

			for (String token : tokens) {
				word.set(token);
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;

			for (IntWritable value : values)
				sum += value.get();

			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "HadoopWordCount");
		job.setJarByClass(HadoopWordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordCount(), args);
		System.exit(ret);
	}
}
