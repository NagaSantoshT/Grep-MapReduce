import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GrepJob implements Tool
{
	private Configuration conf;

	@Override
	public Configuration getConf() 
	{
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) 
	{
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		// TODO Auto-generated method stub
		Job wordCountJob = new Job(getConf());
		wordCountJob.setJobName("Grep Job");
		wordCountJob.setJarByClass(this.getClass());
		
		wordCountJob.setMapperClass(GrepMapper.class);
		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(NullWritable.class);
		
		wordCountJob.setNumReduceTasks(0);
		
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(NullWritable.class);
		
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
		
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return wordCountJob.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new GrepJob(), args);
	}
	
}
