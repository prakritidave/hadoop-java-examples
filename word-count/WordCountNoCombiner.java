package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountNoCombiner {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);	
    private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, 
	InterruptedException {

	StringTokenizer itr = new StringTokenizer(value.toString());		
	
	while (itr.hasMoreTokens()) {
		
	String val= itr.nextToken().toString();

	//take the first character of each word in the line
	char ch = val.charAt(0);

	//choose the word which starts with either m,M,n,N,o,O,p,P,q or Q	
	if(ch=='m' || ch=='M' || ch == 'n' ||ch=='N' || ch == 'o' || ch =='O' || 
	      ch== 'p' ||ch=='P' || ch == 'q'|| ch=='Q')
	{  		

	   word.set(val);
	   context.write(word, one);
	    
	}
	else
		//ignore irrelevant words 
		continue;

	}
	
	
  }

}

	public static class FirstPartitioner extends Partitioner<Text,IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numOfReduceTasks) {
		// TODO Auto-generated method stub
		
		String val= key.toString();
		//get the first char of the given key
		char ch = val.charAt(0);

	// if there is just one reducer return the same partition number.		
		if(numOfReduceTasks == 0)return 0;

	// for more than 1 reducers, return the partition number based on the first character of the key 	
	 	
		if(ch=='m'||ch=='M')
			return 0;
		else if(ch=='n'||ch=='N')
		    return 1;
		else if(ch=='o'||ch=='O')
			return 2;
		else if(ch=='p'||ch=='P')
			return 3;
		else
		    return 4;
		
	}
	}



	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values,Context context) 
			throws IOException, InterruptedException {

	int sum = 0;

	for (IntWritable val : values) {
	 sum += val.get();
	}

	result.set(sum);
	context.write(key, result);

	}

	}


	public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();

	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length != 2) {

	System.err.println("Usage: wordcount <in> <out>");

	System.exit(2);

	}

	Job job = new Job(conf, "word count");

	job.setJarByClass(WordCountNoCombiner.class);

	job.setMapperClass(TokenizerMapper.class);

	//combiner class is disabled
	/*job.setCombinerClass(IntSumReducer.class);*/

	job.setReducerClass(IntSumReducer.class);

	//set number of reduce tasks
	job.setNumReduceTasks(5);

	//set partitioner class
	job.setPartitionerClass(FirstPartitioner.class);

	job.setOutputKeyClass(Text.class);

	job.setOutputValueClass(IntWritable.class);

	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);

	}


}
