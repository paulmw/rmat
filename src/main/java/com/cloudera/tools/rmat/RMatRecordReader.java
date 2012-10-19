package com.cloudera.tools.rmat;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RMatRecordReader extends RecordReader<Edge, NullWritable> {

	private long nodes = -1;
	private long edgesGenerated = 0;
	private long edgesPerSplit = 0;
	private float [] p = {0, 0, 0, 0};

	private Random random;
	
	private Edge edge = new Edge();
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		p[0] = conf.getFloat("rmat.p0", -1.0f);
		p[1] = p[0] + conf.getFloat("rmat.p1", -1.0f);
		p[2] = p[1] + conf.getFloat("rmat.p2", -1.0f);
		p[3] = 1.0f;
		
		verifyConfiguration();
		
		RMatSplit rmatSplit = (RMatSplit) split;
		edgesPerSplit = rmatSplit.getEdgesPerSplit();
		nodes = rmatSplit.getNodes();
		long seed = rmatSplit.getSeed();
		if(seed == -1) {
			random = new Random();
		} else {
			random = new Random(seed);
		}
	}
	
	void verifyConfiguration() {
		if(p[0] == -1.0f) throw new IllegalStateException("p[0] should be defined using conf.setFloat(\"rmat.p0\", p0)");
		if(p[1] == -1.0f) throw new IllegalStateException("p[1] should be defined using conf.setFloat(\"rmat.p1\", p1)");
		if(p[2] == -1.0f) throw new IllegalStateException("p[2] should be defined using conf.setFloat(\"rmat.p2\", p2)");
		if(p[0] < 0f || p[0] > 1f) throw new IllegalStateException("p[0] should be between 0 and 1");
		if(p[1] < 0f || p[1] > 1f) throw new IllegalStateException("p[0] should be between 0 and 1");
		if(p[2] < 0f || p[2] > 1f) throw new IllegalStateException("p[0] should be between 0 and 1");
		if(p[1] < p[0]) throw new IllegalStateException("p[1] should be greater that p[0]");
		if(p[2] < p[1]) throw new IllegalStateException("p[2] should be greater that p[1]");
	}

	int generateQuadrant(float d) {
		if(d < p[0]) return 0;
		if(d < p[1]) return 1;
		if(d < p[2]) return 2;
		return 3;
	}
	
	/*
	 * The following diagram shows how the quadrants are assigned,
	 * and how the parameters relate to the layout.
	 * 
	 *      x0 --- x1
	 *  y0  +---+---+
	 *   |  | 0 | 1 |
	 *   |  +---+---+
	 *   |  | 2 | 3 |
	 *  y1  +---+---+
	 *  
	 */
	 void generateEdge() {
		long x0 = 0, y0 = 0, x1 = nodes, y1 = nodes;
		while(x0 != x1 && y0 != y1) {
			long dx = (long) Math.ceil(((float) x1 - (float) x0) / (float) 2);
			long dy = (long) Math.ceil(((float) y1 - (float) y0) / (float) 2);
			int quadrant = generateQuadrant(random.nextFloat());
			if(quadrant == 0) {
				x1 -= dx;
				y1 -= dy;
			}
			else if(quadrant == 1) {
				x0 += dx;
				y1 -= dy;
			} else if(quadrant == 2) {
				x1 -= dx;
				y0 += dy;
			} else if(quadrant == 3) {
				x0 += dx;
				y1 -= dy;
			} else {
				throw new IllegalStateException("Unknown quadrant: " + quadrant);
			}
		}
		edge.setFrom(x0);
		edge.setTo(y0);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(edgesGenerated < edgesPerSplit) {
			generateEdge();
			edgesGenerated++;
			return true;
		}
		return false;
	}

	@Override
	public Edge getCurrentKey() throws IOException,
			InterruptedException {
		return edge;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float) edgesGenerated / (float) edgesPerSplit;
	}

	@Override
	public void close() throws IOException {
		// NOP
	}

}
