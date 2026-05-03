/**
 *
 *  @author Żuchowski Kacper s33521
 *
 */

package zad1;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class ChatClientTask extends FutureTask<Void> {

    private final ChatClient client;

    private ChatClientTask(ChatClient client, List<String> msgs, int wait) {
        super(createCallable(client, msgs, wait));
        this.client = client;
    }

    public static ChatClientTask create(ChatClient c, List<String> msgs, int wait) {
        return new ChatClientTask(c, msgs, wait);
    }

    public ChatClient getClient() {
        return client;
    }

    private static Callable<Void> createCallable(ChatClient client, List<String> msgs, int wait) {
        return () -> {
            client.login();
            sleep(wait);

            for (String msg : msgs) {
                client.send(msg);
                sleep(wait);
            }

            client.logout();
            sleep(wait);

            return null;
        };
    }

    private static void sleep(int wait) {
        if (wait == 0) {
            return;
        }

        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}