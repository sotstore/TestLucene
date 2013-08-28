/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Random;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Version;

/** Simple command-line based search demo. */
public class ReadFiles {

	private ReadFiles() {
	}
	
	public enum DIRTYPE {
		MMAP, NIO, SIMPLE,
	}
	
	public enum OPTYPE {
		SEQSCAN, SEQSEARCH, RANDFETCH, REPOPENCLOSE,
	}
	
	public static class Result {
		public long initTs = 0;
		public long closeTs = 0;
		public long searchTs = 0;
		public long fetchTs = 0;
		
		public long initTsNr = 0;
		public long closeTsNr = 0;
		public long searchTsNr = 0;
		public long fetchTsNr = 0;
	}
	
	public static Result doScan(String path, DIRTYPE type, IndexReader ir) throws IOException {
		IndexReader reader;
		Result r = new Result();
		long beginTs, endTs;

		if (ir != null) 
			reader = ir;
		else {
			beginTs = System.currentTimeMillis();
			switch (type) {
			default:
			case MMAP:
				reader = DirectoryReader.open(MMapDirectory.open(new File(path)));
				break;
			case NIO:
				reader = DirectoryReader.open(NIOFSDirectory.open(new File(path)));
				break;
			case SIMPLE:
				reader = DirectoryReader.open(SimpleFSDirectory.open(new File(path)));
				break;
			}
			endTs = System.currentTimeMillis();
			r.initTs += endTs - beginTs;
			r.initTsNr += 1;
		}
		
		System.out.println("-----Scan   it------" + reader.maxDoc());
		
		beginTs = System.currentTimeMillis();
		for (int i = 0; i < reader.maxDoc(); i++) {
			Document doc = reader.document(i);
			doc.get("foo");
			doc.get("bar");
			//System.out.println("Key: " + doc.get("foo") + ", Value: " + doc.get("bar") + ", Content: " + doc.get("content"));
		}
		endTs = System.currentTimeMillis();
		r.fetchTs += endTs - beginTs;
		r.fetchTsNr += reader.maxDoc();

		if (ir == null) {
			beginTs = System.currentTimeMillis();
			reader.close();
			endTs = System.currentTimeMillis();
			r.closeTs += endTs - beginTs;
			r.closeTsNr += 1;
		}

		return r;
	}
	
	public static Result doSearch(String path, DIRTYPE type, IndexReader ir) throws IOException {
		IndexReader reader;
		Result r = new Result();
		long beginTs, endTs;
		
		if (ir != null) 
			reader = ir;
		else {
			beginTs = System.currentTimeMillis();
			switch (type) {
			default:
			case MMAP:
				reader = DirectoryReader.open(MMapDirectory.open(new File(path)));
				break;
			case NIO:
				reader = DirectoryReader.open(NIOFSDirectory.open(new File(path)));
				break;
			case SIMPLE:
				reader = DirectoryReader.open(SimpleFSDirectory.open(new File(path)));
				break;
			}
			endTs = System.currentTimeMillis();
			r.initTs += endTs - beginTs;
			r.initTsNr += 1;
		}
		
		System.out.println("-----Search it------");

		IndexSearcher searcher = new IndexSearcher(reader);

		Query q = NumericRangeQuery.newIntRange("foo", new Integer("100000"), null,
				false, false);
		beginTs = System.currentTimeMillis();
		ScoreDoc[] hits = searcher.search(q, searcher.getIndexReader().maxDoc()).scoreDocs;
		endTs = System.currentTimeMillis();
		r.searchTs += endTs - beginTs;
		r.searchTsNr += hits.length;
		System.out.println("Hits -> " + hits.length);

		boolean isSeq = true;
		int lastid = 0;
		beginTs = System.currentTimeMillis();
		for (int i = 0; i < hits.length; i++) {
			if (hits[i].doc < lastid)
				isSeq = false;
			Document doc = searcher.doc(hits[i].doc);
			doc.get("foo");
			doc.get("bar");
			//System.out.println("Key: " + doc.get("foo") + ", Value: " + doc.get("bar"));
		}
		System.out.println("Search DocID is SEQ? " + isSeq);
		endTs = System.currentTimeMillis();
		r.fetchTs += endTs - beginTs;
		r.fetchTsNr += hits.length;
		
		if (ir == null) {
			beginTs = System.currentTimeMillis();
			reader.close();
			endTs = System.currentTimeMillis();
			r.closeTs += endTs - beginTs;
			r.closeTsNr += 1;
		}
		
		return r;
	}

	public static Result doRandFetch(String path, DIRTYPE type, IndexReader ir, int randfetchnr) throws IOException {
		IndexReader reader;
		Result r = new Result();
		long beginTs, endTs;
		
		if (ir != null) 
			reader = ir;
		else {
			beginTs = System.currentTimeMillis();
			switch (type) {
			default:
			case MMAP:
				reader = DirectoryReader.open(MMapDirectory.open(new File(path)));
				break;
			case NIO:
				reader = DirectoryReader.open(NIOFSDirectory.open(new File(path)));
				break;
			case SIMPLE:
				reader = DirectoryReader.open(SimpleFSDirectory.open(new File(path)));
				break;
			}
			endTs = System.currentTimeMillis();
			r.initTs += endTs - beginTs;
			r.initTsNr += 1;
		}
		
		System.out.println("-----RandFt it------");
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Randomized the fetch
		Random rand = new Random();		
		beginTs = System.currentTimeMillis();
		int maxDoc = reader.maxDoc();
		if (randfetchnr > 0) maxDoc = randfetchnr;
		for (int i = 0; i < maxDoc; i++) {
			Document doc = reader.document(rand.nextInt(maxDoc));
			doc.get("foo");
			doc.get("bar");
			//System.out.println("Key: " + doc.get("foo") + ", Value: " + doc.get("bar"));
		}
		endTs = System.currentTimeMillis();
		r.fetchTs += endTs - beginTs;
		r.fetchTsNr += maxDoc;
		
		if (ir == null) {
			beginTs = System.currentTimeMillis();
			reader.close();
			endTs = System.currentTimeMillis();
			r.closeTs += endTs - beginTs;
			r.closeTsNr += 1;
		}
		
		return r;
	}
	
	public static Result doRepOpenClose(String path, DIRTYPE type, long nr) throws IOException {
		IndexReader reader;
		Result r = new Result();
		long beginTs, endTs;

		System.out.println("-----Open/Close it------");

		for (int i = 0; i < nr; i++) {
			beginTs = System.currentTimeMillis();
			switch (type) {
			default:
			case MMAP:
				reader = DirectoryReader.open(MMapDirectory.open(new File(path)));
				break;
			case NIO:
				reader = DirectoryReader.open(NIOFSDirectory.open(new File(path)));
				break;
			case SIMPLE:
				reader = DirectoryReader.open(SimpleFSDirectory.open(new File(path)));
				break;
			}
			endTs = System.currentTimeMillis();
			r.initTs += endTs - beginTs;
			r.initTsNr += 1;
			
			beginTs = System.currentTimeMillis();
			reader.close();
			endTs = System.currentTimeMillis();
			r.closeTs += endTs - beginTs;
			r.closeTsNr += 1;
		}
		
		return r;
	}
	
	public static String genResult(Result r, String title, String cached, String fetchPattern) {
		String str = "";
		
		str += "Result for [" + title + "]\n";
		str += "Init \t(" + cached + ") \tlatency \t" + (double)r.initTs / r.initTsNr + " ms\n";
		str += "Fetch \t(" + fetchPattern + ") \tlatency \t" + (double)r.fetchTs / r.fetchTsNr + " ms, fps " + r.fetchTsNr / ((double)r.fetchTs / 1000) + " /s\n";
		str += "Search \t \t \tlatency \t" + (double)r.searchTs / r.searchTsNr	+ " ms\n";
		str += "Close \t(" + cached + ") \tlatency \t" + (double)r.closeTs / r.closeTsNr + " ms\n";
		return str;
	}
	
	public static class TestThread extends Thread {
		String indexPath;
		OPTYPE optype;
		DIRTYPE dirtype;
		Result r;
		IndexReader ir;
		int randfetchnr = -1;
		
		public TestThread(String threadName, String indexPath, OPTYPE optype, DIRTYPE dirtype, Result r, IndexReader ir) {
			this.indexPath = indexPath;
			this.optype = optype;
			this.dirtype = dirtype;
			this.r = r;
			this.ir = ir;
		}
		
		public void run() {
			Result lr = null;
			
			try {
				switch (optype) {
				case SEQSCAN:
					lr = doScan(indexPath, dirtype, ir);
					break;
				case SEQSEARCH:
					lr = doSearch(indexPath, dirtype, ir);
					break;
				case RANDFETCH:
					lr = doRandFetch(indexPath, dirtype, ir, randfetchnr);
					break;
				case REPOPENCLOSE:
					lr = doRepOpenClose(indexPath, dirtype, 1000);
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (lr != null) {
				synchronized (r) {
					r.initTs += lr.initTs;
					r.initTsNr += lr.initTsNr;
					r.fetchTs += lr.fetchTs;
					r.fetchTsNr += lr.fetchTsNr;
					r.searchTs += lr.searchTs;
					r.searchTsNr += lr.searchTsNr;
					r.closeTs += lr.closeTs;
					r.closeTsNr += lr.closeTsNr;
				}
			}
		}
	}
	
	/** Simple command-line based search demo. */
	public static void main(String[] args) throws Exception {
		String usage = "Usage:\tjava ReadFiles [-index dir] \n\nSee http://lucene.apache.org/java/4_0/demo.html for details.";
		if (args.length > 0
				&& ("-h".equals(args[0]) || "-help".equals(args[0]))) {
			System.out.println(usage);
			System.exit(0);
		}

		String index = "tmp-codec";
		String field = "contents";

		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				index = args[i + 1];
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			}
		}
		
		System.out.println("Please drop cache now ...");
		System.in.read();
		
		
		//System.out.println(genResult(doSearch(index, DIRTYPE.NIO), "SEQ SEARCH", "noncached", "seqskip"));
		//System.out.println(genResult(doScan(index, DIRTYPE.MMAP), "SEQ SCAN", "noncached", "seqscan"));
		//System.out.println(genResult(doRandFetch(index, DIRTYPE.SIMPLE), "RAND FETCH", "noncached", "randfetch"));
		//System.out.println(genResult(doRepOpenClose(index, DIRTYPE.MMAP, 100), "REP On/Ce", "cached", "rep o / c"));
		TestThread[] tts = new TestThread[1];
		Result r = new Result();
		long beginTs, endTs;
		
//		beginTs = System.currentTimeMillis();
//		IndexReader ir = DirectoryReader.open(MMapDirectory.open(new File(index)));
//		endTs = System.currentTimeMillis();
//		r.initTs += endTs - beginTs;
//		r.initTsNr += 1;
		
		for (int i = 0; i < tts.length; i++) {
			tts[i] = new TestThread("TestThread/" + i, index, OPTYPE.RANDFETCH, DIRTYPE.MMAP, r, null);
			tts[i].randfetchnr = 10000;
			tts[i].start();
		}
		for (int i = 0; i < tts.length; i++) {
			tts[i].join();
		}
		
//		beginTs = System.currentTimeMillis();
//		ir.close();
//		endTs = System.currentTimeMillis();
//		r.closeTs += endTs - beginTs;
//		r.closeTsNr += 1;
		
		System.out.println(genResult(r, "2 SEQ SCAN", "noncached", "seqscan"));
		System.out.println(r.initTsNr + ", " + r.closeTsNr + ", " + r.fetchTsNr);
	}
}
