package togos.ccouch3;

import togos.ccouch3.FlowUploader.Sink;
import togos.service.Service;

/**
 * When run, an upload client reads messages
 * from a message queue, uploads objects to the destination
 * (possibly checking if they're there first, possibly using
 * multiple threads and additional queues to accomplish this), 
 * adding URLs to an AddableSet when FullyStoredMarkers are encountered,
 * and quitting when EndMessages are encountered.
 * 
 * Messages must be processed in order, and each message must be
 * completely processed (uploads complete) before the next is processed.
 * e.g. A FullyStoredMarker should not be processed until all
 * preceding uploads are done.  An EndMessage should not kill
 * the service until everything else it was doing is done. 
 */
public interface UploadClient extends Service, Sink<Object> { }
