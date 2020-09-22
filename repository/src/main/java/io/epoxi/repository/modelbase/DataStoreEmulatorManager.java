package io.epoxi.repository.modelbase;

import com.googlecode.objectify.ObjectifyService;

public class DataStoreEmulatorManager {

	public static void initDataStoreEmulator() {
		
		ObjectifyService.init();

		// ObjectifyService.init(new ObjectifyFactory(
		// 		DatastoreOptions.newBuilder()
		// 				.setHost("http://localhost:8484")
		// 				.setProjectId("etlengine")
		// 				.build()
		// 				.getService()
		// ));
	}
}