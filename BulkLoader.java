package assignment182;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BulkLoader 
{
	public static class BulkLoadMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> 
	{  
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();

			//The line is splitting the file records into parts wherever it is comma (‘,’) separated, and the first column are considered as rowKey.
			String[]fields=line.split(",");
			
			String rowKey = fields[0];
			
			ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
			
			Put HPut = new Put(Bytes.toBytes(rowKey));
			
			//This will write the rowKey values into Hbase while creating an object.
			//Here the fields of tables inside Hbase is are stated to be written
			HPut.add(Bytes.toBytes("details"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
			
			HPut.add(Bytes.toBytes("details"), Bytes.toBytes("location"), Bytes.toBytes(fields[2]));
			
			HPut.add(Bytes.toBytes("details"), Bytes.toBytes("age"), Bytes.toBytes(fields[3]));
			
			context.write(HKey,HPut);

		}  
	}
	
	
	@SuppressWarnings("deprecation")
	public static void main( String[] args  ) throws Exception 
	{
		Configuration conf = HBaseConfiguration.create();
		
		HTable table=new HTable( conf,args[1] );
	
		conf.set( "hbase.mapred.outputtable", args[1] );
		
		Job job = new Job(conf,"Bulk_loader");  
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);
		job.setReduceSpeculativeExecution(false);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(HFileOutputFormat.class);
		job.setJarByClass( BulkLoader.class );
		job.setMapperClass(BulkLoader.BulkLoadMap.class);
		
		FileInputFormat.setInputPaths(job, args[0]);
		
		TextOutputFormat.setOutputPath(job, new Path( args[2] ));
		
		HFileOutputFormat.configureIncrementalLoad(job, table);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
