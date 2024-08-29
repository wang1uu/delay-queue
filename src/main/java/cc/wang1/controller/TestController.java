package cc.wang1.controller;

import cc.wang1.adapter.RedissionClientAdapter;
import cc.wang1.queue.DelayQueue;
import cc.wang1.queue.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/test")
public class TestController {

    private final DelayQueue<String> delayQueue;

    @Autowired
    public TestController(RedissionClientAdapter redissionClientAdapter) {
        this.delayQueue = new DelayQueue.Builder<String>()
                .redisClient(redissionClientAdapter)
                .name("test20240819235314")
                .itemIdGenerator(System::currentTimeMillis)
                .increment(5L)
                .batchSize(10L)
                .build();
    }

    @GetMapping("/status")
    public String status() {
        return "ok";
    }

    @GetMapping("/offer")
    public String offer(@RequestParam("v") String v, @RequestParam("e") Long e) {
        long expire = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(e);

        boolean result = delayQueue.offer(expire, v);
        if (!result) {
            throw new RuntimeException(String.format("消息 [%s] 投递时间 [%s] 投递失败", v, LocalDateTime.now()));
        }
        return String.format("消息 [%s] 投递时间 [%s] 到期时间 [%s]",
                v,
                LocalDateTime.now(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(expire), ZoneOffset.of("+8")));
    }

    @GetMapping("/poll")
    public String poll(@RequestParam("w") Long w) {
        Item<String> result = delayQueue.poll(w, TimeUnit.SECONDS);
        if (result == null) {
            return "不存在到期消息";
        }
        return String.format("到期消息 [%s] 获取时间 [%s] 到期时间 [%s]",
                result.getData(),
                LocalDateTime.now(),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(result.getExpiry()), ZoneOffset.of("+8")));
    }
}
