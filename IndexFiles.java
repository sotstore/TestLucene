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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode; import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

import iie.codec.MyCodec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.Random;

/** Index all text files under a directory.
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing.
 * Run it with no command-line arguments for usage information.
 */
public class IndexFiles {
	
  private IndexFiles() {}

  /** Index all text files under a directory. */
  public static void main(String[] args) {
    String usage = "java org.apache.lucene.demo.IndexFiles"
                 + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
                 + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                 + "in INDEX_PATH that can be searched with SearchFiles";
    String indexPath = "tmp-codec";
    int numDocs = 100;
    boolean create = true;
    for(int i=0;i<args.length;i++) {
      if ("-index".equals(args[i])) {
        indexPath = args[i+1];
        i++;
      } else if ("-numdocs".equals(args[i])) {
        numDocs = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-update".equals(args[i])) {
        create = false;
      }
    }

    Date start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");
      
      Directory dir = FSDirectory.open(new File(indexPath));
      Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
      IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_42, analyzer);

      if (create) {
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(OpenMode.CREATE);
        iwc.setCodec(new MyCodec());
      } else {
        // Add new documents to an existing index:
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

      IndexWriter writer = new IndexWriter(dir, iwc);
      indexDocs(writer, numDocs);

      // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here.  This can be
      // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
      // writer.forceMerge(1);

      writer.close();

      Date end = new Date();
      System.out.println(end.getTime() - start.getTime() + " total milliseconds");

    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() +
       "\n with message: " + e.getMessage());
    }
  }

  static void indexDocs(IndexWriter writer, int numDocs)
    throws IOException {
	if (numDocs <= 0)
		return;

	Random rand = new Random();
	System.out.println("Generate " + numDocs + " KV pairs...");
	long beginTs = System.currentTimeMillis();
	for (int i = 0; i < numDocs; i++) {
		int j;
		Document doc = new Document();
		j = rand.nextInt();
		IntField nf = new IntField("foo", j, Field.Store.YES);

		doc.add(nf);
		double d = rand.nextDouble();
		DoubleField df = new DoubleField("d1", d, Field.Store.YES);
		doc.add(df);
		
		doc.add(new Field("bar", "hello, world; 我们是中文呀! 我要去棉山旅游，身体健康，万事如意。请尽快联系，速回电，去吃饭！" + j, Field.Store.YES,
				Field.Index.ANALYZED));
		String vv = "URL http://www.google.com; http://www.baidu.com";
		doc.add(new Field("content", vv, Field.Store.YES, Field.Index.NO));

		writer.addDocument(doc);
	}
	long endTs = System.currentTimeMillis();
	System.out.println("Index latency: " + (endTs - beginTs) / (double)numDocs + " ms.");
  }
}
