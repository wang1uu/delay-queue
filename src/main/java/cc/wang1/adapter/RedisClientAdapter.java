package cc.wang1.adapter;

import cc.wang1.queue.Item;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface RedisClientAdapter<T> {

    boolean zAdd(String zSet, long score, Item<T> value);

    Item<T> zFirst(String zSet);

    Item<T> lPoll(String zSet, long timeout, TimeUnit timeUnit);

    long executeTransferScript(String script, List<String> keys, Object... args);
}
