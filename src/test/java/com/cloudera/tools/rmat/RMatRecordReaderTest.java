package com.cloudera.tools.rmat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class RMatRecordReaderTest {

	/**
	 * This tests that the quadrant generator produces selections in the correct
	 * proportions.
	 * 
	 * This works by defining proportions, then simulating over many tests to check
	 * that we get what was expected.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testQuadrants() throws IOException, InterruptedException {
		// Simulation parameters
		float [] p = {0.7f, 0.15f, 0.1f, 0.05f};
		int tests = 10000;
		
		// Build the RecordReader
		Configuration conf = new Configuration();
		conf.setFloat("rmat.p0", p[0]);
		conf.setFloat("rmat.p1", p[1]);
		conf.setFloat("rmat.p2", p[2]);
		RMatRecordReader reader = new RMatRecordReader();
		TaskAttemptContext context = mock(TaskAttemptContext.class);
		when(context.getConfiguration()).thenReturn(conf);
		reader.initialize(new RMatSplit(-1, 100, 10), context);
		
		// Perform the simulation
		Random random = new Random();
		int [] results = {0,0,0,0};
		for (int i = 0; i < tests; i++) {
			results[reader.generateQuadrant(random.nextFloat())]++;
		}
		int sum = results[0] + results[1] + results[2] + results[3];
		
		// Check that the quadrants came back with the correct proportions
		assertEquals(p[0], (float) results[0] / (float) sum, 0.01);
		assertEquals(p[1], (float) results[1] / (float) sum, 0.01);
		assertEquals(p[2], (float) results[2] / (float) sum, 0.01);
		assertEquals(p[3], (float) results[3] / (float) sum, 0.01);
	}
	
	
	/**
	 * This tests that the correct proportion of (0,0) edges are generated.
	 * 
	 * This works by calculating the probability of a (0,0) edge being generated,
	 * and comparing that to the actual number of (0,0) edges in a simulation. 
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void testExpected() throws IOException, InterruptedException {
		
		// Simulation parameters
		float [] p = {0.7f, 0.15f, 0.1f, 0.05f};
		int nodes = 1000;
		int edges = 1000000;
		
		// This is the probability we expect to see if things are working correctly
		float calculatedProbability = (float) Math.pow(p[0], Math.log(nodes) / Math.log(2));
		
		// Build the RecordReader
		Configuration conf = new Configuration();
		conf.setFloat("rmat.p0", p[0]);
		conf.setFloat("rmat.p1", p[1]);
		conf.setFloat("rmat.p2", p[2]);
		RMatRecordReader reader = new RMatRecordReader();
		TaskAttemptContext context = mock(TaskAttemptContext.class);
		when(context.getConfiguration()).thenReturn(conf);
		reader.initialize(new RMatSplit(-1, nodes, edges), context);
		
		Edge zeroZero = new Edge(0, 0);
		
		// Run the simulation, counting up the number of (0,0) edges
		int count = 0;
		while(reader.nextKeyValue()) {
			if(reader.getCurrentKey().equals(zeroZero)) {
				count++;
			}
		}
		
		// Calculate the estimated probability
		float estimatedProbability = ((float) count / (float) edges);
		
		// The important bit :)
		assertTrue(estimatedProbability / calculatedProbability > 0.95);
	}
	
	@Test
	public void test0000() throws IOException, InterruptedException {
		// This should test what happens when a quadrant is 0,0,0,0;
	}

}
