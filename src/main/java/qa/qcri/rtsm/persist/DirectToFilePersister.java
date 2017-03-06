package qa.qcri.rtsm.persist;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import qa.qcri.rtsm.util.Util;

public class DirectToFilePersister extends WriteOnlyPersister {
	private String baseOutputFilename;

	public void setBaseOutputFilename(String baseOutputFilename) {
		this.baseOutputFilename = baseOutputFilename;
	}

	@Override
	public void set(String key, Object value, int persistTime) throws InterruptedException {
//		Util.logInfo(this, "key:" + key+" |value:"+(String)value);		
		if (value instanceof String) {
			set(key, (String) value);
		} else {
			throw new IllegalArgumentException("Expected a String value");
		}
		
		
	}

	private void set(String key, String value) {
		File outputFile = new File(baseOutputFilename + key);
		String dirName = outputFile.getParent();
		//if(key.contains("dohanews.co")){
		//	Util.logInfo(this, "key:" + key+" |basefile:"+ baseOutputFilename + "|dirName:"+dirName);		
		//	Util.logInfo(this, "Tweets value: " + value );		
		//}
		// Create necessary directory (and parents, if necessary)
		if( dirName != null ) {
			File dir = new File(dirName);
			if( ! dir.exists() ) {
				dir.mkdirs();
				if( ! dir.exists() ) {
					Util.logError(this, "Failed to create directory " + dirName );
				}
			}
		}
		
		// Write file
		try {
			FileUtils.write(outputFile, value);
		} catch (IOException e) {
			Util.logError(this, "Failed to write file " + outputFile );
			e.printStackTrace();
		}
	}
}
