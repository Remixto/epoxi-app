package io.epoxi.app.util.gson;

import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtils {

    private GsonUtils()
    {}

    private static final GsonBuilder gsonBuilder = new GsonBuilder()
            .setPrettyPrinting()
            .excludeFieldsWithoutExposeAnnotation()
		    .setPrettyPrinting().serializeNulls();

    public static void registerType(
            RuntimeTypeAdapterFactory<?> adapter) {
                        gsonBuilder.registerTypeAdapterFactory(adapter);
    }

    public static Gson getGson(FieldNamingStrategy namingStrategy) {

        gsonBuilder.setFieldNamingStrategy(namingStrategy);
        return gsonBuilder.create();
    }
}
