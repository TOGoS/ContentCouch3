package togos.ccouch3.repo;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import togos.blob.ByteBlob;
import togos.blob.ByteChunk;

public class LoggingRepository implements Repository
{
	public enum EventType {
		GET,
		PUT,
		CONTAINS,
	}
	
	public class Event {
		public final EventType type;
		public final String urn;
		public final boolean urnProvidedInInput;
		public final boolean success;
		public Event( EventType type, String urn, boolean urnProvidedInInput, boolean success ) {
			this.type = type;
			this.urn = urn;
			this.urnProvidedInInput = urnProvidedInInput;
			this.success = success;
		}
	}
	
	public List<Event> events = new ArrayList<Event>();
	
	protected final Repository backingRepository;
	public LoggingRepository(Repository backingRepository) {
		this.backingRepository = backingRepository;
	}
	
	@Override public ByteBlob getBlob(String urn) throws IOException {
		boolean success = false;
		try {
			ByteBlob b = backingRepository.getBlob(urn);
			success = true;
			return b;
		} finally {
			events.add(new Event(EventType.GET, urn, true, success));
		}
	}
	
	@Override public boolean contains(String urn) {
		boolean success = false;
		try {
			boolean c = backingRepository.contains(urn);
			success = true;
			return c;
		} finally {
			events.add(new Event(EventType.CONTAINS, urn, true, success));
		}
	}
	
	@Override
	public ByteChunk getChunk(String urn, int maxSize) {
		boolean success = false;
		try {
			ByteChunk bc = backingRepository.getChunk(urn, maxSize);
			success = true;
			return bc;
		} finally {
			events.add(new Event(EventType.GET, urn, true, success));
		}
	}
	
	@Override
	public File getFile(String urn) throws IOException {
		boolean success = false;
		try {
			File f = backingRepository.getFile(urn);
			success = true;
			return f;
		} finally {
			events.add(new Event(EventType.GET, urn, true, success));
		}
	}
	
	@Override
	public InputStream getInputStream(String urn) throws IOException {
		boolean success = false;
		try {
			InputStream is = backingRepository.getInputStream(urn);
			success = true;
			return is;
		} finally {
			events.add(new Event(EventType.GET, urn, true, success));
		}
	}
	
	@Override public String put(InputStream is) throws StoreException {
		boolean success = false;
		String urn = null;
		try {
			urn = backingRepository.put(is);
			success = true;
			return urn;
		} finally {
			events.add(new Event(EventType.CONTAINS, urn, false, success));
		}
	}
	
	@Override public void put(String urn, InputStream is) throws StoreException {
		boolean success = false;
		try {
			backingRepository.put(urn, is);
			success = true;
		} finally {
			events.add(new Event(EventType.CONTAINS, urn, true, success));
		}
	}
}
