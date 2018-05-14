package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {

		public static void main(String[] args) throws Exception {

		Job job = Job.getInstance();
		job.setJarByClass(Graph.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/f0"));
		job.waitForCompletion(true);

		for(int runner=0;runner<5;runner++) {
		 Job job2 = Job.getInstance();
		 job2.setJarByClass(Graph.class);
		 job2.setInputFormatClass(SequenceFileInputFormat.class);
		 job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		 job2.setMapOutputKeyClass(LongWritable.class);
		 job2.setMapOutputValueClass(Vertex.class);
		 job2.setOutputKeyClass(LongWritable.class);
		 job2.setOutputValueClass(Vertex.class);
		 job2.setMapperClass(Mapper2.class); 
		 job2.setReducerClass(Reducer2.class);
		 FileInputFormat.addInputPath(job2,new Path(args[1]+("/f")+runner));
		 FileOutputFormat.setOutputPath(job2, new Path(args[1]+("/f")+(runner+1)));
		 job2.waitForCompletion(true);
		}
		
		Job job3 = Job.getInstance();
		 job3.setJarByClass(Graph.class);
		 job3.setInputFormatClass(SequenceFileInputFormat.class);
		 job3.setOutputFormatClass(TextOutputFormat.class);
		 job3.setMapOutputKeyClass(LongWritable.class);
		 job3.setMapOutputValueClass(LongWritable.class);
		 job3.setOutputKeyClass(LongWritable.class);
		 job3.setOutputValueClass(LongWritable.class);
		 job3.setMapperClass(Mapper3.class); 
		 job3.setReducerClass(Reducer3.class);
		 FileInputFormat.addInputPath(job3,new Path(args[1]+"/f5"));
		 FileOutputFormat.setOutputPath(job3, new Path(args[2]));
		 job3.waitForCompletion(true);
		 
	}

	public static class Vertex implements Writable {

		public short tag;
		public long group;
		public long VID;
		public Vector<Long> adjacent = new Vector<Long>();

		public Vertex() {

		}

		public Vertex(short v, long x, long y, Vector<Long> z) {

			tag = v;
			group = x;
			VID = y;
			adjacent = z;

		}

		public Vertex(Vertex j) {
			tag = j.tag;
			group = j.group;
			VID = j.VID;
			adjacent = j.adjacent;
		}

		public Vertex(short tag, long group) {

			this.tag = tag;
			this.group = group;
		}

		@Override
		public void readFields(DataInput in) throws IOException {

			adjacent.clear();
			tag = in.readShort();
			group = in.readLong();
			VID = in.readLong();
			long count = in.readLong();
			for (int i = 0; i < count; i++)
				adjacent.addElement(in.readLong());
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeShort(tag);
			out.writeLong(group);
			out.writeLong(VID);
			out.writeLong(adjacent.size());
			for (int i = 0; i < adjacent.size(); i++)
				out.writeLong(adjacent.get(i));

		}
		
		@Override
		public String toString() {
			return tag+","+VID+","+group+","+adjacent;
		}

	}

	public static class Mapper1 extends Mapper<Object, Text, LongWritable, Vertex> {

		Vector<Long> read_vector = new Vector<Long>();

		@Override
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {

			read_vector.clear();
			Scanner s = new Scanner(values.toString()).useDelimiter(",");

			short x = 0;
			long i = new Long(s.nextInt());

			while (s.hasNextInt()) {
				read_vector.add(new Long(s.nextInt()));

			}
			context.write(new LongWritable(i), new Vertex(x, i, i, read_vector));
			s.close();
		}
	}

	public static class Reducer1 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {

			for (Vertex v : values)
				context.write(key, v);
		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {


		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {


			
			context.write(new LongWritable(value.VID), value);
			
			
			for(Long x: value.adjacent) {
				context.write(new LongWritable(x), new Vertex((short)1,value.group));
			}
			
		}
	}

	public static class Reducer2 extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		
		
		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			
			Vector<Long> adj = new Vector<Long>();
			long m = Long.MAX_VALUE;
		
			for (Vertex v : values) {
				
				
				if (v.tag == (short)0) {
					
					adj = (Vector<Long>) v.adjacent.clone();
					
				}
				 m = Math.min(m, v.group);
			}
			context.write(key, new Vertex((short) 0, m, key.get(), adj));
		}
		
	}
	
	
	public static class Mapper3 extends Mapper<LongWritable, Vertex, LongWritable, LongWritable> {
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {
	
			context.write(new LongWritable(value.group), new LongWritable(1));
		}
	}
	
	
	public static class Reducer3 extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			long sum=0;
			for (LongWritable v : values) {
				sum=sum+1;
			}
			context.write( key ,new LongWritable(sum));
				
		}
		
	}
	

}
