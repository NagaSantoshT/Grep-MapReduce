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
		Job grepJob = new Job(getConf());
		grepJob.setJobName("Grep Job");
		grepJob.setJarByClass(this.getClass());
		
		grepJob.setMapperClass(GrepMapper.class);
		grepJob.setMapOutputKeyClass(Text.class);
		grepJob.setMapOutputValueClass(NullWritable.class);
		
		grepJob.setNumReduceTasks(0);
		
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(NullWritable.class);
		
		grepJob.setInputFormatClass(TextInputFormat.class);
		grepJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(grepJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(grepJob, new Path(args[1]));
		
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return grepJob.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new GrepJob(), args);
	}
	
}
