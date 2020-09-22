package io.epoxi.repository.api;

import com.googlecode.objectify.ObjectifyService;

public class App {

    public static void main(String[] args) {

        init();
    }

    public static void init()
    {
        if (Boolean.TRUE.equals(initialized)) return;  //Tests use this class.  Multiple unit tests will call this repeatably so lets leave early if the job is done.

        ObjectifyService.init();
        initialized = true;
    }

    static Boolean initialized = false;
}
