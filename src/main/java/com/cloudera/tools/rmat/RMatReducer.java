package com.cloudera.tools.rmat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

public class RMatReducer extends Reducer<Edge, NullWritable, Edge, NullWritable>{

	private Configuration conf;
	private Edge written = null;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		conf = context.getConfiguration();
	}

	@Override
	protected void reduce(Edge edge, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		for(NullWritable value : values) {
			if(written == null || !edge.equals(written)) {
				written = WritableUtils.clone(edge, conf);
				context.write(edge, value);
			}
		}
		
		written = null;
	}

	
	
}
