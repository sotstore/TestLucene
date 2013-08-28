package iie.codec;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene40.Lucene40StoredFieldsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

public class MyCodec extends FilterCodec {
	private StoredFieldsFormat sff;
	
	public MyCodec() {
		super("MyCodec", new Lucene42Codec());
		//sff = new CompressingStoredFieldsFormat("MyCompressingStoredFieldsFormat", CompressionMode.FAST_DECOMPRESSION, 16 * 1024);
		sff = new Lucene40StoredFieldsFormat(); 
	}

	public StoredFieldsFormat storedFieldsFormat() {
		return sff;
	}
}
