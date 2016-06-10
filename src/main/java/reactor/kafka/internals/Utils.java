package reactor.kafka.internals;

import java.util.concurrent.ThreadFactory;

public class Utils {

    public static ThreadFactory newThreadFactory(String name) {
        return new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setName(name);
                return thread;
            }
        };
    }
}
