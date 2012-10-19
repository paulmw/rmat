package com.cloudera.tools.rmat;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

public class RMatSplitTest {

	@Test
	public void testSerde() throws IOException {
		RMatSplit split = new RMatSplit(-1, 7, 11);
		DataOutputBuffer out = new DataOutputBuffer();
		split.write(out);
		assertEquals(24, out.getLength());
		
		DataInputBuffer in = new DataInputBuffer();
		in.reset(out.getData(), out.getLength());
		RMatSplit split2 = new RMatSplit();
		split2.readFields(in);
		assertEquals(-1, split2.getSeed());
		assertEquals(7, split2.getNodes());
		assertEquals(11, split2.getEdgesPerSplit());
	}

}
