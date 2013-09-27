package togos.ccouch3.slf;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import togos.blob.ByteChunk;
import togos.blob.SimpleByteChunk;

/**
 * Yet another key-value store, designed for speed and simplicity.
 * To cut down on multiple reads, references to chunks contain the chunk's
 * length; this way, once you have a reference to a chunk, it can be
 * read all at once.
 * 
 * Header format:
 *   "SLF2"
 *   "INDX" <64-bit index chunk reference>
 *   "RECL" <64-bit recycle list reference>
 *   "EHDR" ; indicates end of header entries
 * 
 * Chunk reference format (64 bits, total):
 *   <8 reserved bits> <40-bit offset> <16-bit size>
 *   
 *   'size' usually means number of bytes in the target chunk,
 *   but in the case of the index chunk it's log2(number of index entries).
 *   i.e. index size in bytes = 8 * (1 << size).
 *   
 *   Offsets and sizes are unsigned.  A 'null pointer' is represented
 *   by all 64 bits being zero.
 * 
 * The index chunk is simply a list of list chunk references (8 bytes each).
 * 
 * List chunk format:
 *   <64-bit reference to next chunk> <payload>
 * 
 * Key/value chunk payload format:
 *   "PAIR" <16-bit key length> <16-bit data length> <key> <data> <possible garbage>
 * 
 * Chunks in the recycle list have no format; the data they contain is garbage.
 */
public class SimpleListFile2 implements Flushable, Closeable, SimpleMap<ByteChunk,ByteChunk>
{
	protected static void mkParentDirs( File f ) {
		File p = f.getParentFile();
		if( p != null && !p.exists() ) p.mkdirs();
	}
	
	public static SimpleListFile2 mkSlf( File f ) {
		mkParentDirs(f);
		try {
			return new SimpleListFile2( new RandomAccessFileBlob(f, "rw"), 16, true );
		} catch( FileNotFoundException e ) {
			// This should not happen!
			throw new RuntimeException(e);
		}
	}
	
	static final byte[] slf2 = {'S','L','F','2'};
	static final byte[] indx = {'I','N','D','X'};
	static final byte[] recl = {'R','E','C','L'};
	static final byte[] ehdr = {'E','H','D','R'};
	static final byte[] pair = {'P','A','I','R'};
	static final byte[] recy = {'R','E','C','Y'};
	
	final RandomAccessBlob blob;
	final FileChannel fileChannel;
	
	long indexOffset;
	int indexSize; // in bytes, not entries! Used to load the entire thing during bulk operations
	int indexMask;
	int reclRefOffset;
	
	public SimpleListFile2( RandomAccessBlob b, int defaultIndexSizePower, boolean useFileLocks ) {
		this.blob = b;
		if( useFileLocks && b instanceof RandomAccessFile ) {
			fileChannel = ((RandomAccessFile)b).getChannel();
		} else {
			fileChannel = null;
		}
		
		if( defaultIndexSizePower < 0 || defaultIndexSizePower > 17 ) {
			throw new RuntimeException("Index size power should be 0-17.  Given: "+defaultIndexSizePower);
		}
		
		init( defaultIndexSizePower );
	}
	
	protected static final void copy( byte[] src, byte[] buf, int offset ) {
		for( int i=0; i<src.length; ++i ) {
			buf[offset++] = src[i];
		}
	}
	
	protected static final void copy( byte[] src, int srcOffset, byte[] dst, int dstOffset, int len ) {
		for( len -= 1; len >= 0; --len ) {
			dst[dstOffset++] = src[srcOffset++];
		}
	}
	
	public static final boolean equals( byte[] b1, int o1, byte[] b2, int o2, int length ) {
		for( int j=length-1; j>=0; --j ) {
			if( b1[j+o1] != b2[j+o2] ) return false;
		}
		return true;
	}
	
	public static final boolean equals( byte[] b1, byte[] b2, int o2 ) {
		return equals( b1, 0, b2, o2, b1.length );
	}
	
	protected static final void encodeShort( short v, byte[] buf, int offset ) {
		buf[offset+0] = (byte)(v>>8);
		buf[offset+1] = (byte)(v>>0);
	}
	
	public static final void encodeLong( long v, byte[] buf, int offset ) {
		buf[offset+0] = (byte)(v>>56);
		buf[offset+1] = (byte)(v>>48);
		buf[offset+2] = (byte)(v>>40);
		buf[offset+3] = (byte)(v>>32);
		buf[offset+4] = (byte)(v>>24);
		buf[offset+5] = (byte)(v>>16);
		buf[offset+6] = (byte)(v>> 8);
		buf[offset+7] = (byte)(v>> 0);
	}
	
	protected static final short decodeShort( byte[] buf, int offset ) {
		return (short)(
			((short)(buf[offset+0] & 0xFF) <<  8) |
			((short)(buf[offset+1] & 0xFF) <<  0)
		);
	}
	
	public static final long decodeLong( byte[] buf, int offset ) {
		return
			((long)(buf[offset+0] & 0xFF) << 56) |
			((long)(buf[offset+1] & 0xFF) << 48) |
			((long)(buf[offset+2] & 0xFF) << 40) |
			((long)(buf[offset+3] & 0xFF) << 32) |
			((long)(buf[offset+4] & 0xFF) << 24) |
			((long)(buf[offset+5] & 0xFF) << 16) |
			((long)(buf[offset+6] & 0xFF) <<  8) |
			((long)(buf[offset+7] & 0xFF) <<  0);
	}
	
	protected static final long chunkRef( long offset, int size ) {
		return
			((offset & 0xFFFFFFFFFFl) << 16) |
			((size & 0xFFFF) << 0);
	}
	
	protected static final long refOffset( long ref ) {
		return (ref >> 16) & 0xFFFFFFFFFFl;
	}
	
	protected static final int refSize( long ref ) {
		return (int)(ref & 0xFFFF);
	}
	
	protected static final int indexMask( int sizePower ) {
		return (1<<sizePower)-1;
	}
	
	protected static final int indexSize( int sizePower ) {
		return (1<<sizePower)<<3;
	}
	
	protected void init( int defaultIndexSizePower ) {
		int headerSize = 32; // "SLF2" + "INDX" + chunkRef + "RECY" + chunkRef + "EHDR"
		if( blob.getSize() == 0 ) {
			indexOffset = headerSize;
			indexMask = indexMask(defaultIndexSizePower);
			indexSize = indexSize(defaultIndexSizePower);
			reclRefOffset = 20; 
					
			int headerAndIndexSize = headerSize + ((1 << defaultIndexSizePower) << 3);
			byte[] headerAndIndex = new byte[headerAndIndexSize];
			copy( slf2, headerAndIndex, 0 );
			copy( indx, headerAndIndex, 4 );
			encodeLong( chunkRef(headerSize,defaultIndexSizePower), headerAndIndex, 8 );
			copy( recl, headerAndIndex, 16 );
			encodeLong( 0, headerAndIndex, 20 );
			copy( ehdr, headerAndIndex, 28 );
			blob.put(0, new SimpleByteChunk(headerAndIndex));
		} else {
			ByteChunk headerBlob = blob.get( 0, headerSize );
			if( headerBlob.getSize() < headerSize ) {
				throw new RuntimeException("Couldn't read entire "+headerSize+"-byte SLF2 header (read "+headerBlob.getSize()+" bytes)");
			}
			byte[] buf = headerBlob.getBuffer();
			int o = headerBlob.getOffset();
			if( !equals(slf2, buf, o+0 ) ) {
				throw new RuntimeException("Found invalid SLF2 magic");
			}
			o = 4;
			while( o < headerSize && !equals(ehdr, buf, o)  ) {
				if( equals(indx, buf, o) ) {
					long indxRef = decodeLong( buf, o+4 );
					indexOffset = refOffset(indxRef);
					int indexSizePower = refSize(indxRef);
					indexMask = indexMask(indexSizePower);
					indexSize = indexSize(indexSizePower);
					o += 12;
				} else if( equals(recl, buf, o) ) {
					reclRefOffset = o+4;
					o += 12;
				} else {
					o += 4;
				}
				
			}
		}
	}
	
	protected long getLong( long offset ) {
		ByteChunk c = blob.get( offset, 8 );
		return decodeLong( c.getBuffer(), c.getOffset() );
	}
	
	protected void putLong( long offset, long value ) {
		ByteChunk c = new SimpleByteChunk(new byte[8]);
		encodeLong( value, c.getBuffer(), 0 );
		blob.put( offset, c );
	}
	
	protected ByteChunk getChunk( long ref ) {
		return blob.get(refOffset(ref), refSize(ref));
	}
	
	/*
	 *  0 - next reference
	 *  8 - "PAIR"
	 * 12 - key length
	 * 14 - value length
	 * 16 - key
	 * 16 + key length - value
	 */
	
	protected static final boolean pairMatches( ByteChunk chunk, ByteChunk key ) {
		byte[] buf = chunk.getBuffer();
		int o = chunk.getOffset();
		
		if( !equals(pair, buf, o+8) ) return false;
		short cKeyLen = decodeShort( buf, o+12 );
		if( cKeyLen != key.getSize() ) return false;
		return equals( key.getBuffer(), key.getOffset(), buf, o+16, cKeyLen );
	}
	
	protected static final ByteChunk pairValue( long chunkOffset, ByteChunk chunk ) {
		byte[] buf = chunk.getBuffer();
		int o = chunk.getOffset();
		short cKeyLen = decodeShort( buf, o+12 );
		short cValLen = decodeShort( buf, o+14 );
		if( 16 + cKeyLen + cValLen > chunk.getSize() ) {
			System.err.println("Tried to read value from malformed pair chunk at "+chunkOffset);
			return null;
		}
		return new SimpleByteChunk( buf, o + 16 + cKeyLen, cValLen );
	}
	
	protected static final long next( ByteChunk chunk ) {
		return decodeLong( chunk.getBuffer(), chunk.getOffset() );
	}
	
	/**
	 * Return the byte position within the index chunk of the reference
	 * in the index to the specified key
	 */
	protected int indexSubPos( ByteChunk key ) {
		return (key.hashCode() & indexMask) << 3;
	}
	
	/**
	 * Return the byte position within the file of the reference in
	 * the index to the specified key
	 */
	protected long indexPos( ByteChunk key ) {
		return indexSubPos(key) + indexOffset;
	}
	
	protected static final int encodedPairSize( ByteChunk key, ByteChunk value ) {
		return 16+key.getSize()+value.getSize();
	}
	
	protected static final void encodePairData( ByteChunk key, ByteChunk value, byte[] buffer, int offset ) {
		copy( pair, buffer, offset+8 );
		encodeShort( (short)key.getSize(), buffer, offset+12 );
		encodeShort( (short)value.getSize(), buffer, offset+14 );
		copy( key.getBuffer(), key.getOffset(), buffer, offset+16, key.getSize() );
		copy( value.getBuffer(), key.getOffset(), buffer, offset+16+key.getSize(), value.getSize() );
	}
	
	protected static final void encodePair( long next, ByteChunk key, ByteChunk value, byte[] buffer, int offset ) {
		encodeLong( next, buffer, offset+0 );
		encodePairData( key, value, buffer, offset );
	}
	
	protected static final ByteChunk encodePair( long next, ByteChunk key, ByteChunk value ) {
		byte[] buffer = new byte[16+key.getSize()+value.getSize()];
		encodePair( next, key, value, buffer, 0 );
		return new SimpleByteChunk( buffer );
	}
	
	//// 
	
	protected void _putWithoutLocking( long indexPos, ByteChunk key, ByteChunk value ) {
		long oldList = getLong( indexPos );
		
		ByteChunk pair = encodePair( oldList, key, value );
		long newListOffset = blob.getSize();
		// TODO: May want to adjust the location of the new chunk
		// to avoid crossing 4kB boundaries for possible slight performance
		// improvement on certain filesystems?
		blob.put( newListOffset, pair );
		putLong( indexPos, chunkRef(newListOffset, pair.getSize()) );
	}
	
	//// The public interface ////
	
	public void put( ByteChunk key, ByteChunk value ) {
		// TODO: replace old values, use the recycle list, etc
		long indexPos = indexPos(key);
		
		FileLock fl = null;
		synchronized( this ) {
			try {
				try {
					if( fileChannel != null ) fl = fileChannel.lock(0, Long.MAX_VALUE, false);
					
					_putWithoutLocking( indexPos, key, value );
				} finally {
					if( fl != null ) fl.release();
				}
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void bulkPut1( ByteChunk[] keys, ByteChunk[] values, int offset, int count ) {
		long[] indexPos = new long[count];
		for( int i=0; i<count; ++i ) {
			indexPos[i] = indexPos(keys[i]); 
		}
		
		FileLock fl = null;
		synchronized( this ) {
			try {
				try {
					if( fileChannel != null ) fl = fileChannel.lock(0, Long.MAX_VALUE, false);
					
					for( int i=0; i<count; ++i ) {
						_putWithoutLocking( indexPos[i], keys[i], values[i] );
					}
				} finally {
					if( fl != null ) fl.release();
				}
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 * Writes new data to the file using one read and two writes.
	 * Can be more efficient than bulkPut1 when
	 * - a large number of puts can be combined, and
	 * - the index is small.
	 */
	public void bulkPut2( ByteChunk[] keys, ByteChunk[] values, int offset, int count ) {
		int size = 0;
		int[] sizes = new int[count];
		int[] offsets = new int[count];
		int[] indexSubPos = new int[count];
		for( int i=0; i<count; ++i ) {
			offsets[i] = size; 
			size += (sizes[i] = encodedPairSize( keys[offset+i], values[offset+i] ));
			indexSubPos[i] = indexSubPos(keys[offset+i]);
		}
		byte[] buffer = new byte[size];
		
		for( int i=0; i<count; ++i ) {
			encodePairData( keys[offset+i], values[offset+i], buffer, offsets[i] );
		}
		
		// Now that all the easy stuff's out of the way, do the I/O stuff
		
		FileLock fl = null;
		synchronized( this ) {
			try {
				try {
					if( fileChannel != null ) fl = fileChannel.lock(0, Long.MAX_VALUE, false);
					
					long newDataOffset = blob.getSize();
					
					// Load entire index
					ByteChunk index = blob.get( indexOffset, indexSize );
					for( int i=0; i<count; ++i ) {
						// Copy old list reference into new data
						copy( index.getBuffer(), index.getOffset()+indexSubPos[i], buffer, offsets[i], 8 );
						// Replace old list reference with reference to new chunk
						encodeLong( chunkRef( newDataOffset+offsets[i], sizes[i] ), index.getBuffer(), index.getOffset()+indexSubPos[i] );
					}
					
					// Save new data
					blob.put( newDataOffset, new SimpleByteChunk(buffer) );
					
					// Save entire index
					blob.put( indexOffset, index );
				} finally {
					if( fl != null ) fl.release();
				}
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 * Attempts to do multiple puts using the most efficient method
	 * according to some heuristic.
	 * 
	 * Based on testing on my 64-bit Dell running Windows 7, bulkPut and
	 * multiple puts are about the same efficiency at:
	 * 
	 * Index length
	 *      | Batch size (without locking)
	 *      |     |    Batch size (with locking)
	 *      |     |     |
	 *	65536    64    32
	 *  32768    32    16
	 *  16384    10		6
	 *  
	 * So it seems putAll seems to stop being worthwhile when
	 *  index length > 1000 * batch size (without locking), or
	 *  index length > 2000 * batch size (with locking)
	 */
	public void smartPut( ByteChunk[] keys, ByteChunk[] values, int offset, int count ) {
		if( (fileChannel == null && indexSize >  8000 * count) ||
			(fileChannel != null && indexSize > 16000 * count) ) {
			bulkPut1( keys, values, offset, count );
		} else {
			bulkPut2( keys, values, offset, count );
		}
	}
	
	public void multiPut( ByteChunk[] keys, ByteChunk[] values, int offset, int count ) {
		smartPut( keys, values, offset, count );
	}
	
	public ByteChunk get( ByteChunk key ) {
		long indexPos = indexPos(key);
		
		FileLock fl = null;
		synchronized( this ) {
			try {
				try {
					if( fileChannel != null ) fl = fileChannel.lock(0, Long.MAX_VALUE, true);
					
					long chunkOffset = getLong( indexPos );
					while( chunkOffset != 0 ) {
						ByteChunk c = getChunk( chunkOffset );
						if( pairMatches( c, key ) ) {
							ByteChunk v = pairValue( chunkOffset, c );
							if( v != null ) return v;
						}
						chunkOffset = next(c);
					}
					
					return null;
				} finally {
					if( fl != null ) {
						fl.release();
						// On Windows 7 at least, this seems to be sufficient to
						// constructively work on the same file from multiple processes
						// even though the file will be reported by `dir` with its
						// old size until it is closed by a process.
						//
						// i.e. force(...) does not seem necessary for
						// synchronization between processes, which is good because
						// it's terribly slow.
					}
				}
			} catch( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void multiGet( ByteChunk[] keys, ByteChunk[] values, int offset, int count ) {
		for( int i=0; i<count; ++i ) {
			values[offset+i] = get(keys[offset+i]);
		}
	}
	
	/**
	 * Force changes to go be physically written to disk.
	 * This may be mislabeled, as 'flush' usually just means sent to the OS...
	 **/
	public void flush() throws IOException {
		if( this.fileChannel != null ) this.fileChannel.force(true);
	}
	
	public void close() throws IOException {
		if( this.fileChannel != null ) this.fileChannel.close();
	}
}
