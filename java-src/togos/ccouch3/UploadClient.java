package togos.ccouch3;

import togos.ccouch3.FlowUploader.Sink;
import togos.service.Service;

public interface UploadClient extends Service, Sink<Object>
{

}
