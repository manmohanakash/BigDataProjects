package edu.uta.cse6331;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiply {

	
	public static void main(String[ ] args) throws Exception {
		
		Configuration conf = new Configuration();
			
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job1 = Job.getInstance(conf, "Map reduce part 1");
		
		job1.setJarByClass(Multiply.class);

		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Elem.class);

		job1.setOutputKeyClass(Pair.class);
		job1.setOutputValueClass(DoubleWritable.class);
	
		job1.setReducerClass(Reducer1.class);

		job1.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, MapperM.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, MapperN.class);
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance();
		 

		job2.setJobName("Map reduce part 2");
		job2.setJarByClass(Multiply.class);
		
		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

		job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	     
	    job2.setMapperClass(Mapper2.class);
	    job2.setReducerClass(Reducer2.class);
	     
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	     
	    TextInputFormat.addInputPath(job2, new Path(args[2]));
	    FileOutputFormat.setOutputPath(job2,new Path(args[3]));
	     
	    job2.waitForCompletion(true);
	     
	}
	
	
	public static class Elem implements Writable {

		public short tag;
		public int index;
		public double value;
		
		
		public Elem() {	}

		public Elem(Elem a) {
		tag=a.tag;
		index=a.index;
		value=a.value;
		}
		
		public Elem(short a, int b, double c) {
			tag = a;
			index = b;
			value = c;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeShort(tag);
			out.writeInt(index);
			out.writeDouble(value);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			tag = in.readShort();
			index = in.readInt();
			value = in.readDouble();
		}
	
	}

	public static class Pair implements WritableComparable<Pair>  {

		public int i, j;
		
		public Pair() {}
		
		public Pair(Pair x) {
			i=x.i;
			j=x.j;
		}

		public Pair(int a, int b) {
			i = a;
			j = b;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			i=in.readInt();
			j=in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(i);
			out.writeInt(j);
		}

		@Override
		public int compareTo(Pair o) {
			return (i == o.i) ? j-o.j : i-o.i;

		}
	
		
	  @Override
		public String toString() {
			    return i+","+j;
		}
	  
	  @Override
	  	public int hashCode() {
		  return (i+","+j).hashCode();
	  }
						
	}
	
	public static class MapperM extends Mapper<Object, Text, IntWritable, Elem> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			@SuppressWarnings("resource")
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			
			short x = 0;
			int i = s.nextInt();
			int j = s.nextInt();
			double v = s.nextDouble();
			

			context.write(new IntWritable(j), new Elem(x,i,v));
			s.close();
		}
	}

	public static class MapperN extends Mapper<Object, Text, IntWritable, Elem> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			@SuppressWarnings("resource")
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			
			short x = 1;
			int i = s.nextInt();
			int j = s.nextInt();
			double v = s.nextDouble();
			

			context.write(new IntWritable(i), new Elem(x,j,v));
			s.close();
		}
	}

	public static class Reducer1 extends Reducer<IntWritable, Elem, Pair,DoubleWritable > {

		public Vector<Elem>A = new Vector<Elem>();
		public Vector<Elem>B = new Vector<Elem>();
		
		public void reduce(IntWritable key, Iterable<Elem> values, Context context)
				throws IOException, InterruptedException {

			A.clear();
			B.clear();
			
			for (Elem v: values) {
                if (v.tag == 0)
                    A.add(new Elem(v.tag,v.index,v.value));
                else 
                	B.add(new Elem(v.tag,v.index,v.value));           
			}
			
            		
                
			for (Elem a: A) {
				for (Elem b: B) {
					context.write(new Pair(a.index,b.index),new DoubleWritable(a.value*b.value));
				}
			}
		
		}
		
	}

	public static class Mapper2 extends Mapper<Object, Text, Pair,DoubleWritable> { 
	  	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	  	
	  		   //context.write( key, new DoubleWritable(value)); 
	  		 

	 			@SuppressWarnings("resource")
	 			Scanner s = new Scanner(value.toString()).useDelimiter(",");
	 			
	 			int i = s.nextInt();
	 			int j = s.nextInt();
	 			double v = s.nextDouble();
	 			
	 			context.write(new Pair(i,j), new DoubleWritable(v));
	 			s.close();
	 		
		  } 	  
	}
  
  	public static class Reducer2 extends Reducer<Pair, DoubleWritable, Text,Text> {
  
  	public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws
  			IOException, InterruptedException {
  			
  			int a=key.i,b=key.j;
  			double sum = 0.0;
  			for (DoubleWritable i : values) { 
  				sum += i.get();  				
  			}
  			String temp =a+","+b+","+sum;
  			context.write(new Text(temp),null);
  		} 
  	} 
		

}
