package togos.ccouch3;

/**
 * Indicates a method for scanning blobs for references to other blobs,
 * e.g. to recursively download them.
 * 
 * Note that the names of these are used to find some SLF cache files,
 * so don't go changing them around willy-nilly.
 */
enum BlobReferenceScanMode {
	NEVER, // Never scan blobs
	TEXT   // Scan text blobs for 'urn:....'
}
