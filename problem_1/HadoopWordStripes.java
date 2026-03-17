import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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

public class HadoopWordStripes extends Configured implements Tool {

	private final static IntWritable one = new IntWritable(1);
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

	public static class Stripe extends MapWritable {

		public void add(String w) {
			IntWritable count = new IntWritable(0);
			if (containsKey(new Text(w))) {
				count = (IntWritable) get(new Text(w));
				remove(new Text(w));
			}
			count = new IntWritable(count.get() + one.get());
			put(new Text(w), count);
		}

	    public void merge(Stripe from) {
	        for (Writable fromKey : from.keySet())
	            if (containsKey(fromKey))
	                put(fromKey, new IntWritable(((IntWritable) get(fromKey)).get() + ((IntWritable) from.get(fromKey)).get()));
	            else
	                put(fromKey, (IntWritable) from.get(fromKey));
	    }

	    @Override
	    public String toString() {
	        StringBuilder buffer = new StringBuilder();
	        for (Writable key : keySet())
	            buffer.append(" ").append(key).append(":").append(get(key));
	        return buffer.toString();
	    }
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Stripe> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> tokens = extractValidTokens(value.toString());

			for (int i = 0; i < tokens.size(); i++) {
				
				Stripe stripe = new Stripe();
				String currentToken = tokens.get(i);
				String neighbor;

				if (i > 0) {
					neighbor = tokens.get(i - 1);
					stripe.add(neighbor);
				}

				if (i < tokens.size() - 1) {
					neighbor = tokens.get(i + 1);
					stripe.add(neighbor);
				}

				context.write(new Text(currentToken), stripe);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Stripe, Text, Stripe> {

		@Override
		public void reduce(Text key, Iterable<Stripe> values, Context context)
				throws IOException, InterruptedException {
			
			Stripe globalStripe = new Stripe();

			for (Stripe localStripe : values)
				globalStripe.merge(localStripe);
			
			context.write(key, globalStripe);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "HadoopWordStripes");
		job.setJarByClass(HadoopWordStripes.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Stripe.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(), new HadoopWordStripes(), args);
		System.exit(ret);
	}
}
