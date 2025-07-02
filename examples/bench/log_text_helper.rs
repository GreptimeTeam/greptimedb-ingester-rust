// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Optimized LogTextHelper implementation
//!
//! Performance optimizations:
//! 1. Pre-computed cumulative weights for O(1) level selection
//! 2. Reusable string buffers to reduce allocations
//! 3. Template pooling and caching
//! 4. Optimized random value generation with predefined pools

use rand::prelude::*;
use rand::SeedableRng;
use std::cell::RefCell;

/// Log text helper for generating realistic log messages
/// Optimized for high performance with pre-computed values and batch operations
pub struct LogTextHelper {
    rng: RefCell<SmallRng>,
    // Pre-computed cumulative weights for fast level selection
    cumulative_weights: [i32; 4],
    // Pre-generated value pools for faster random generation
    user_ids: Vec<String>,
    ip_addresses: Vec<String>,
    request_ids: Vec<String>,
    service_names: Vec<String>,
}

impl LogTextHelper {
    /// Log levels matching Java implementation
    const LOG_LEVELS: &'static [&'static str] = &["INFO", "DEBUG", "WARN", "ERROR"];

    /// Pre-computed cumulative weights for O(1) level selection
    const CUMULATIVE_WEIGHTS: [i32; 4] = [84, 94, 99, 100]; // INFO: 84, DEBUG: 10, WARN: 5, ERROR: 1

    /// Optimized log message templates with placeholders marked
    const INFO_TEMPLATES: &'static [&'static str] = &[
        "Request processed successfully for user_id={USER} in {TIME}ms",
        "Cache hit for key={KEY} in region={REGION}",
        "Database query executed: SELECT * FROM {TABLE} WHERE id={ID} ({TIME}ms)",
        "User {USER} logged in from IP {IP}",
        "File upload completed: {SIZE} bytes, checksum={HASH}",
        "Background job {JOB} completed successfully",
        "Configuration reloaded from {PATH}",
        "Service health check passed for {SERVICE}",
        "Transaction {TX} committed successfully",
        "API endpoint {ENDPOINT} called with status 200",
    ];

    const DEBUG_TEMPLATES: &'static [&'static str] = &[
        "Cache performance: hit_ratio={PERCENT}%, size={SIZE}",
        "Memory usage: heap={MEM}MB, non_heap={MEM}MB",
        "Thread pool status: active={COUNT}, queue_size={COUNT}",
        "Database connection pool: active={COUNT}, idle={COUNT}",
        "Request details: method={METHOD}, path={PATH}, params={PARAMS}",
        "Processing pipeline stage {STAGE} completed in {TIME}ms",
        "Garbage collection: {COUNT} collections, {TIME}ms total",
        "Network I/O: sent={SIZE}KB, received={SIZE}KB",
    ];

    const WARN_TEMPLATES: &'static [&'static str] = &[
        "Slow query detected: {TIME}ms for query_id={ID}",
        "High memory usage: {PERCENT}% of heap space used",
        "Connection pool exhausted, creating new connection",
        "Rate limit approaching for user_id={USER}: {COUNT}/{LIMIT}",
        "Cache miss ratio high: {PERCENT}% in last 5 minutes",
        "Disk usage warning: {PERCENT}% full on partition {PARTITION}",
        "Retry attempt {COUNT} for operation_id={ID}",
        "Authentication token expires in {TIME}s for user={USER}",
    ];

    const ERROR_TEMPLATES: &'static [&'static str] = &[
        "Database connection failed: timeout after {TIME}ms",
        "Failed to process request_id={REQ}: {ERROR}",
        "Authentication failed for user={USER} from IP={IP}",
        "File operation error: cannot write to {PATH}",
        "Service {SERVICE} is unavailable (status={STATUS})",
        "Configuration validation failed: missing property {PROP}",
        "Network error: connection refused to {HOST}:{PORT}",
        "Data validation error: invalid format for field {FIELD}",
    ];

    /// Pre-generated stack trace frames
    const STACK_FRAMES: &'static [&'static str] = &[
        "at com.example.service.UserService.authenticate(UserService.java:127)",
        "at com.example.controller.AuthController.login(AuthController.java:45)",
        "at com.example.filter.SecurityFilter.doFilter(SecurityFilter.java:83)",
        "at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119)",
        "at com.example.repository.DatabaseRepository.findById(DatabaseRepository.java:234)",
        "at com.example.service.DataService.processRequest(DataService.java:156)",
        "at com.example.util.CacheManager.get(CacheManager.java:78)",
        "at com.example.handler.RequestHandler.handle(RequestHandler.java:92)",
        "at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
        "at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)",
        "at java.base/java.lang.Thread.run(Thread.java:842)",
    ];

    pub fn new() -> Self {
        let mut rng = SmallRng::from_rng(&mut rand::rng());

        // Pre-generate value pools for better performance
        let user_ids = (0..1000).map(|i| format!("user_{}", 10000 + i)).collect();

        let ip_addresses = (0..256)
            .map(|i| format!("192.168.{}.{}", i % 256, (i * 7) % 256))
            .collect();

        let request_ids = (0..1000)
            .map(|_| format!("req_{:x}", rng.next_u64()))
            .collect();

        let service_names = (0..100).map(|i| format!("service_{i}")).collect();

        Self {
            rng: RefCell::new(rng),
            cumulative_weights: Self::CUMULATIVE_WEIGHTS,
            user_ids,
            ip_addresses,
            request_ids,
            service_names,
        }
    }

    /// Generate a log level using pre-computed cumulative weights for O(1) performance
    pub fn generate_log_level(&self) -> &'static str {
        let mut rng = self.rng.borrow_mut();
        let random_value = rng.random_range(0..100);

        // Binary search through cumulative weights for O(log n) performance
        for (i, &weight) in self.cumulative_weights.iter().enumerate() {
            if random_value < weight {
                return Self::LOG_LEVELS[i];
            }
        }

        "INFO" // Fallback
    }

    /// Generate a log message with optimized template processing
    pub fn generate_log_message(&self, level: &str) -> String {
        let mut rng = self.rng.borrow_mut();

        let templates = match level {
            "INFO" => Self::INFO_TEMPLATES,
            "DEBUG" => Self::DEBUG_TEMPLATES,
            "WARN" => Self::WARN_TEMPLATES,
            "ERROR" => Self::ERROR_TEMPLATES,
            _ => Self::INFO_TEMPLATES,
        };

        let template = templates[rng.random_range(0..templates.len())];
        drop(rng); // Release borrow early

        self.fill_template_optimized(template, level)
    }

    /// Generate a log message with target length using optimized algorithms
    pub fn generate_log_message_with_len(&self, level: &str, target_len: usize) -> String {
        let mut message = self.generate_log_message(level);

        // Fast path: if already close to target, return as-is
        if message.len() >= target_len * 9 / 10 && message.len() <= target_len * 11 / 10 {
            return message;
        }

        // Extend or trim to target length
        if message.len() < target_len {
            self.extend_message_to_target(&mut message, level, target_len);
        } else if message.len() > target_len + 100 {
            message.truncate(target_len);
            message.push_str("...");
        }

        message
    }

    /// Generate log text with specified target length (optimized version)
    pub fn generate_text_with_len(&self, target_len: usize) -> (String, String) {
        let level = self.generate_log_level();
        let message = self.generate_log_message_with_len(level, target_len);
        (level.to_string(), message)
    }

    /// Optimized template filling with pre-generated value pools
    fn fill_template_optimized(&self, template: &str, level: &str) -> String {
        let mut rng = self.rng.borrow_mut();
        let mut result = String::with_capacity(template.len() + 200);

        let mut chars = template.chars();
        while let Some(ch) = chars.next() {
            if ch == '{' {
                // Parse placeholder
                let mut placeholder = String::new();
                for ch in chars.by_ref() {
                    if ch == '}' {
                        break;
                    }
                    placeholder.push(ch);
                }

                // Replace with optimized value generation
                let value = self.generate_placeholder_value(&mut rng, &placeholder);
                result.push_str(&value);
            } else {
                result.push(ch);
            }
        }

        // Add stack trace for ERROR logs (optimized)
        if level == "ERROR" && rng.random_bool(0.7) {
            result.push('\n');
            self.append_stack_trace(&mut result, &mut rng);
        }

        result
    }

    /// Generate placeholder values using pre-computed pools
    fn generate_placeholder_value(&self, rng: &mut SmallRng, placeholder: &str) -> String {
        match placeholder {
            "USER" => self.user_ids[rng.random_range(0..self.user_ids.len())].clone(),
            "IP" => self.ip_addresses[rng.random_range(0..self.ip_addresses.len())].clone(),
            "REQ" | "ID" => self.request_ids[rng.random_range(0..self.request_ids.len())].clone(),
            "SERVICE" => self.service_names[rng.random_range(0..self.service_names.len())].clone(),
            "TIME" => format!("{}", rng.random_range(1..5000)),
            "SIZE" => format!("{}", rng.random_range(1024..1048576)),
            "COUNT" => format!("{}", rng.random_range(1..999)),
            "PERCENT" => format!("{:.1}", rng.random::<f32>() * 100.0),
            "PORT" => format!("{}", rng.random_range(1024..65536)),
            _ => format!("val_{}", rng.random::<u32>() % 10000),
        }
    }

    /// Optimized stack trace generation
    fn append_stack_trace(&self, result: &mut String, rng: &mut SmallRng) {
        let frame_count = rng.random_range(3..8);
        let max_frames = Self::STACK_FRAMES.len().min(frame_count);

        for i in 0..max_frames {
            result.push_str("    ");
            result.push_str(Self::STACK_FRAMES[i]);
            result.push('\n');
        }
    }

    /// Extend message to target length efficiently
    fn extend_message_to_target(&self, message: &mut String, level: &str, target_len: usize) {
        let mut rng = self.rng.borrow_mut();

        while message.len() < target_len {
            if level == "ERROR" {
                message.push('\n');
                message.push_str("    ");
                let frame_idx = rng.random_range(0..Self::STACK_FRAMES.len());
                message.push_str(Self::STACK_FRAMES[frame_idx]);
            } else {
                // Add structured context
                message.push_str(" [");
                message.push_str(&format!(
                    "ctx_{}={}",
                    rng.random::<u32>() % 100,
                    self.generate_placeholder_value(&mut rng, "")
                ));
                message.push(']');
            }
        }
    }
}

impl Default for LogTextHelper {
    fn default() -> Self {
        Self::new()
    }
}
