import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
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

public class HadoopPartB extends Configured implements Tool {

	private static final Pattern SPLIT_PATTERN = Pattern.compile("[^a-z0-9.-]+");
	private static final Pattern WORD_PATTERN = Pattern.compile("[a-z-]{6,24}");
	private static final Pattern NUMBER_PATTERN = Pattern.compile("-?[0-9.]{4,16}");

	private static final String WORD_TAG = "W";
	private static final String WORD_PAIR_TAG = "PWW";
	private static final String NUMBER_WORD_PAIR_TAG = "PNW";
	private static final int WINDOW = 2;

	private static boolean isWord(String token) {
		return WORD_PATTERN.matcher(token).matches();
	}

	private static boolean isNumber(String token) {
		return NUMBER_PATTERN.matcher(token).matches();
	}

	private static List<String> extractValidTokens(String line) {
		String lowerLine = line.toLowerCase(Locale.ROOT);
		String[] rawTokens = SPLIT_PATTERN.split(lowerLine);
		List<String> validTokens = new ArrayList<String>();

		for (String rawToken : rawTokens) {
			if (rawToken.isEmpty()) {
				continue;
			}

			if (isWord(rawToken) || isNumber(rawToken)) {
				validTokens.add(rawToken);
			}
		}

		return validTokens;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private final Text outputKey = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = extractValidTokens(value.toString());

			for (String token : tokens) {
				if (isWord(token)) {
					outputKey.set(WORD_TAG + "\t" + token);
					context.write(outputKey, ONE);
				}
			}

			for (int i = 0; i < tokens.size(); i++) {
				String first = tokens.get(i);

				for (int distance = 1; distance <= WINDOW; distance++) {
					int j = i + distance;
					if (j >= tokens.size()) {
						break;
					}

					String second = tokens.get(j);

					if (isWord(first) && isWord(second)) {
						outputKey.set(WORD_PAIR_TAG + "\t" + first + ":" + second);
						context.write(outputKey, ONE);
					}

					if (isNumber(first) && isWord(second)) {
						outputKey.set(NUMBER_WORD_PAIR_TAG + "\t" + first + ":" + second);
						context.write(outputKey, ONE);
					}
				}
			}
		}
	}

	public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	private static class FrequencyEntry {
		private final String token;
		private final int count;

		FrequencyEntry(String token, int count) {
			this.token = token;
			this.count = count;
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		private static final int TOP_K = 100;

		private final PriorityQueue<FrequencyEntry> topWords = new PriorityQueue<FrequencyEntry>(
				TOP_K,
				new Comparator<FrequencyEntry>() {
					@Override
					public int compare(FrequencyEntry a, FrequencyEntry b) {
						if (a.count != b.count) {
							return Integer.compare(a.count, b.count);
						}
						return b.token.compareTo(a.token);
					}
				});

		private final PriorityQueue<FrequencyEntry> topNumberWordPairs = new PriorityQueue<FrequencyEntry>(
				TOP_K,
				new Comparator<FrequencyEntry>() {
					@Override
					public int compare(FrequencyEntry a, FrequencyEntry b) {
						if (a.count != b.count) {
							return Integer.compare(a.count, b.count);
						}
						return b.token.compareTo(a.token);
					}
				});

		private final Text outputKey = new Text();

		private static boolean isBetter(FrequencyEntry candidate, FrequencyEntry currentMin) {
			if (candidate.count != currentMin.count) {
				return candidate.count > currentMin.count;
			}
			return candidate.token.compareTo(currentMin.token) < 0;
		}

		private static void offerTop(PriorityQueue<FrequencyEntry> heap, String token, int count) {
			FrequencyEntry candidate = new FrequencyEntry(token, count);

			if (heap.size() < TOP_K) {
				heap.offer(candidate);
				return;
			}

			FrequencyEntry currentMin = heap.peek();
			if (currentMin != null && isBetter(candidate, currentMin)) {
				heap.poll();
				heap.offer(candidate);
			}
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			String keyString = key.toString();
			int separator = keyString.indexOf('\t');
			if (separator < 0) {
				return;
			}

			String keyType = keyString.substring(0, separator);
			String token = keyString.substring(separator + 1);

			if (WORD_TAG.equals(keyType)) {
				offerTop(topWords, token, sum);
				if (sum == 1000) {
					outputKey.set("TASK1\t" + token);
					context.write(outputKey, new IntWritable(sum));
				}
				return;
			}

			if (WORD_PAIR_TAG.equals(keyType)) {
				if (sum == 1000) {
					outputKey.set("TASK2\t" + token);
					context.write(outputKey, new IntWritable(sum));
				}
				return;
			}

			if (NUMBER_WORD_PAIR_TAG.equals(keyType)) {
				offerTop(topNumberWordPairs, token, sum);
			}
		}

		private void emitTop(String taskTag, PriorityQueue<FrequencyEntry> heap, Context context)
				throws IOException, InterruptedException {
			List<FrequencyEntry> entries = new ArrayList<FrequencyEntry>(heap);
			Collections.sort(entries, new Comparator<FrequencyEntry>() {
				@Override
				public int compare(FrequencyEntry a, FrequencyEntry b) {
					if (a.count != b.count) {
						return Integer.compare(b.count, a.count);
					}
					return a.token.compareTo(b.token);
				}
			});

			for (FrequencyEntry entry : entries) {
				outputKey.set(taskTag + "\t" + entry.token);
				context.write(outputKey, new IntWritable(entry.count));
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			emitTop("TASK3", topWords, context);
			emitTop("TASK4", topNumberWordPairs, context);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: HadoopPartB <input> <output>");
			return 1;
		}

		Job job = Job.getInstance(getConf(), "HadoopPartB");
		job.setJarByClass(HadoopPartB.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopPartB(), args);
		System.exit(ret);
	}
}
