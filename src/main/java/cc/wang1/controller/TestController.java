package cc.wang1.controller;

import cc.wang1.adapter.RedissionClientAdapter;
import cc.wang1.dto.Result;
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
import java.util.Optional;
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
    public Result offer(@RequestParam("v") String v, @RequestParam("e") Long e) {
        long expire = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(e);
        boolean result = delayQueue.offer(expire, v);

        return Result.builder()
                .result(result)
                .message(v)
                .deliveredTime(LocalDateTime.now())
                .expire(LocalDateTime.ofInstant(Instant.ofEpochMilli(expire), ZoneOffset.of("+8")))
                .expireTimestamp(expire)
                .build();
    }

    @GetMapping("/poll")
    public Result poll(@RequestParam("w") Long w) {
        Item<String> result = delayQueue.poll(w, TimeUnit.SECONDS);

        if (result == null) {
            return Result.builder().result(true).build();
        }

        long touchedTimestamp = System.currentTimeMillis();
        LocalDateTime touchedTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(touchedTimestamp), ZoneOffset.of("+8"));

        return Result.builder()
                .result(true)
                .message(result.getData())
                .touchedTime(touchedTime)
                .touchedTimestamp(touchedTimestamp)
                .expire(LocalDateTime.ofInstant(Instant.ofEpochMilli(result.getExpiry()), ZoneOffset.of("+8")))
                .expireTimestamp(result.getExpiry())
                .build();
    }
}
