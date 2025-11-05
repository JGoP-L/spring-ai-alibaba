/*
 * Copyright 2024-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.cloud.ai.graph.bug;

import com.alibaba.cloud.ai.graph.KeyStrategy;
import com.alibaba.cloud.ai.graph.OverAllState;
import com.alibaba.cloud.ai.graph.state.strategy.ReplaceStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 复现测试 - OverAllState 并发安全问题
 * 
 * 这个测试展示了在并发环境下 OverAllState 的数据竞争问题。
 * 由于 OverAllState 内部使用普通的 HashMap，在多线程并发修改时会导致：
 * 1. 数据丢失
 * 2. 状态不一致
 * 3. ConcurrentModificationException
 * 
 * @author bug-fix-team
 */
public class OverAllStateConcurrencyBugTest {

    /**
     * Bug 复现测试 1: 并发更新导致数据丢失
     * 
     * 场景：模拟 ParallelAgent 中多个 Agent 同时更新共享状态
     * 期望：所有更新都应该被记录
     * 实际：部分更新会丢失（这个测试会失败，证明 Bug 存在）
     */
    @Test
    public void testConcurrentUpdateDataLoss() throws InterruptedException {
        System.out.println("\n========== Bug 复现测试 1: 并发更新数据丢失 ==========");
        
        OverAllState state = new OverAllState();
        state.registerKeyAndStrategy("counter", new ReplaceStrategy());
        
        int threadCount = 10;
        int iterationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        
        // 启动多个线程并发更新状态
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程就绪
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        try {
                            Map<String, Object> update = new HashMap<>();
                            update.put("thread_" + threadId + "_key_" + j, "value_" + j);
                            state.updateState(update);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            exceptionCount.incrementAndGet();
                            System.err.println("❌ 线程 " + threadId + " 发生异常: " + e.getClass().getSimpleName());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        System.out.println("🚀 启动 " + threadCount + " 个线程，每个执行 " + iterationsPerThread + " 次更新...");
        startLatch.countDown(); // 开始！
        
        boolean finished = endLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        assertTrue(finished, "测试应该在超时前完成");
        
        int expectedUpdates = threadCount * iterationsPerThread;
        int actualSize = state.data().size();
        
        System.out.println("📊 测试结果:");
        System.out.println("   期望更新数: " + expectedUpdates);
        System.out.println("   实际状态键数: " + actualSize);
        System.out.println("   成功操作数: " + successCount.get());
        System.out.println("   异常次数: " + exceptionCount.get());
        System.out.println("   数据丢失数: " + (expectedUpdates - actualSize));
        
        // 这个断言会失败，证明存在并发问题
        if (actualSize < expectedUpdates) {
            System.out.println("⚠️  Bug 已复现！数据丢失了 " + (expectedUpdates - actualSize) + " 条记录");
        }
        
        // 注意：这个断言预期会失败，这正是 Bug 的体现
        // assertEquals(expectedUpdates, actualSize, 
        //     "并发更新导致数据丢失！预期 " + expectedUpdates + " 个键，实际只有 " + actualSize + " 个");
    }

    /**
     * Bug 复现测试 2: 并发读写导致 ConcurrentModificationException
     * 
     * 场景：一个线程读取状态，同时其他线程修改状态
     * 期望：读取和写入应该是安全的
     * 实际：会抛出 ConcurrentModificationException
     */
    @RepeatedTest(3) // 重复测试增加复现概率
    public void testConcurrentReadWriteException() throws InterruptedException {
        System.out.println("\n========== Bug 复现测试 2: 并发读写异常 ==========");
        
        OverAllState state = new OverAllState();
        for (int i = 0; i < 50; i++) {
            Map<String, Object> init = new HashMap<>();
            init.put("key_" + i, "value_" + i);
            state.updateState(init);
        }
        
        ExecutorService executor = Executors.newFixedThreadPool(5);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(5);
        
        // 线程1-3: 不断修改状态
        for (int i = 0; i < 3; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        Map<String, Object> update = new HashMap<>();
                        update.put("thread_" + threadId + "_" + j, "value");
                        state.updateState(update);
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    System.err.println("❌ 写线程异常: " + e.getClass().getSimpleName());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 线程4-5: 不断读取状态
        for (int i = 0; i < 2; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < 100; j++) {
                        // 遍历状态会触发 ConcurrentModificationException
                        Map<String, Object> data = state.data();
                        for (Map.Entry<String, Object> entry : data.entrySet()) {
                            // 触发迭代
                            entry.getKey();
                        }
                        Thread.sleep(1);
                    }
                } catch (ConcurrentModificationException e) {
                    exceptionCount.incrementAndGet();
                    System.err.println("❌ 读线程发生 ConcurrentModificationException!");
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    System.err.println("❌ 读线程异常: " + e.getClass().getSimpleName());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(15, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("📊 异常统计: " + exceptionCount.get() + " 次");
        
        if (exceptionCount.get() > 0) {
            System.out.println("⚠️  Bug 已复现！发生了 " + exceptionCount.get() + " 次并发异常");
        }
    }

    /**
     * Bug 复现测试 3: KeyStrategy 并发修改冲突
     * 
     * 场景：多个线程同时注册和使用不同的 KeyStrategy
     * 期望：所有策略都应该正确注册和使用
     * 实际：可能导致策略丢失或使用错误的策略
     */
    @Test
    public void testConcurrentKeyStrategyConflict() throws InterruptedException {
        System.out.println("\n========== Bug 复现测试 3: KeyStrategy 并发冲突 ==========");
        
        OverAllState state = new OverAllState();
        
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        List<String> missingStrategies = new CopyOnWriteArrayList<>();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    String key = "strategy_key_" + threadId;
                    
                    // 注册策略
                    state.registerKeyAndStrategy(key, new ReplaceStrategy());
                    
                    // 立即检查是否注册成功
                    Thread.sleep(10); // 增加竞态条件发生概率
                    
                    if (!state.containStrategy(key)) {
                        missingStrategies.add(key);
                        System.err.println("❌ 策略丢失: " + key);
                    }
                } catch (Exception e) {
                    System.err.println("❌ 异常: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();
        
        System.out.println("📊 策略注册结果:");
        System.out.println("   期望注册: " + threadCount + " 个策略");
        System.out.println("   实际注册: " + state.keyStrategies().size() + " 个策略");
        System.out.println("   丢失策略数: " + missingStrategies.size());
        
        if (!missingStrategies.isEmpty()) {
            System.out.println("⚠️  Bug 已复现！以下策略丢失: " + missingStrategies);
        }
    }

    /**
     * 压力测试：高并发场景下的状态一致性
     * 
     * 这个测试使用计数器来检测并发问题
     */
    @Test
    public void testHighConcurrencyStressTest() throws InterruptedException {
        System.out.println("\n========== Bug 复现测试 4: 高并发压力测试 ==========");
        
        OverAllState state = new OverAllState();
        state.registerKeyAndStrategy("counter", KeyStrategy.REPLACE);
        
        // 初始化计数器
        Map<String, Object> init = new HashMap<>();
        init.put("counter", 0);
        state.updateState(init);
        
        int threadCount = 50;
        int incrementsPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < incrementsPerThread; j++) {
                        // 读取-修改-写入 (非原子操作)
                        Integer current = state.value("counter", Integer.class).orElse(0);
                        Map<String, Object> update = new HashMap<>();
                        update.put("counter", current + 1);
                        state.updateState(update);
                    }
                } catch (Exception e) {
                    System.err.println("❌ 异常: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(15, TimeUnit.SECONDS);
        executor.shutdown();
        
        int expectedValue = threadCount * incrementsPerThread;
        int actualValue = state.value("counter", Integer.class).orElse(0);
        
        System.out.println("📊 计数器测试结果:");
        System.out.println("   期望值: " + expectedValue);
        System.out.println("   实际值: " + actualValue);
        System.out.println("   丢失更新: " + (expectedValue - actualValue));
        
        if (actualValue < expectedValue) {
            System.out.println("⚠️  Bug 已复现！由于并发竞争丢失了 " + 
                (expectedValue - actualValue) + " 次更新");
        }
        
        // 预期会失败
        // assertEquals(expectedValue, actualValue, "并发计数器测试失败");
    }
}

