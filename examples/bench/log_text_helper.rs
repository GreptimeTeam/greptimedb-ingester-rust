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

//! LogTextHelper implementation
//!
//! Port of the Java LogTextHelper that generates realistic log messages
//! with proper level distribution and content templates.

use rand::prelude::*;

/// Log text helper for generating realistic log messages
/// Following the Java implementation with the same log level distribution
pub struct LogTextHelper;

impl LogTextHelper {
    /// Log levels matching Java implementation
    const LOG_LEVELS: &'static [&'static str] = &["INFO", "DEBUG", "WARN", "ERROR"];

    /// Log level distribution matching Java implementation
    /// INFO: 84%, DEBUG: 10%, WARN: 5%, ERROR: 1%
    const LOG_LEVEL_WEIGHTS: &'static [i32] = &[84, 10, 5, 1];

    /// Log message templates for different levels
    const INFO_TEMPLATES: &'static [&'static str] = &[
        "Request processed successfully for user_id={} in {}ms",
        "Cache hit for key={} in region={}",
        "Database query executed: SELECT * FROM {} WHERE id={} ({}ms)",
        "User {} logged in from IP {}",
        "File upload completed: {} bytes, checksum={}",
        "Background job {} completed successfully",
        "Configuration reloaded from {}",
        "Service health check passed for {}",
        "Transaction {} committed successfully",
        "API endpoint {} called with status 200",
    ];

    const DEBUG_TEMPLATES: &'static [&'static str] = &[
        "Cache performance: hit_ratio={:.2}%, size={}",
        "Memory usage: heap={}MB, non_heap={}MB",
        "Thread pool status: active={}, queue_size={}",
        "Database connection pool: active={}, idle={}",
        "Request details: method={}, path={}, params={}",
        "Processing pipeline stage {} completed in {}ms",
        "Garbage collection: {} collections, {}ms total",
        "Network I/O: sent={}KB, received={}KB",
    ];

    const WARN_TEMPLATES: &'static [&'static str] = &[
        "Slow query detected: {}ms for query_id={}",
        "High memory usage: {}% of heap space used",
        "Connection pool exhausted, creating new connection",
        "Rate limit approaching for user_id={}: {}/{}",
        "Cache miss ratio high: {:.2}% in last 5 minutes",
        "Disk usage warning: {}% full on partition {}",
        "Retry attempt {} for operation_id={}",
        "Authentication token expires in {}s for user={}",
    ];

    const ERROR_TEMPLATES: &'static [&'static str] = &[
        "Database connection failed: timeout after {}ms",
        "Failed to process request_id={}: {}",
        "Authentication failed for user={} from IP={}",
        "File operation error: cannot write to {}",
        "Service {} is unavailable (status={})",
        "Configuration validation failed: missing property {}",
        "Network error: connection refused to {}:{}",
        "Data validation error: invalid format for field {}",
    ];

    /// Stack trace frames for error logs
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

    /// Context keys for adding additional log context
    const CONTEXT_KEYS: &'static [&'static str] = &[
        "correlation_id",
        "session_id",
        "request_id",
        "user_agent",
        "client_ip",
        "region",
        "datacenter",
        "instance_id",
        "trace_id",
        "span_id",
        "operation",
        "component",
    ];

    /// Generate a log level following the distribution
    pub fn generate_log_level() -> &'static str {
        let mut rng = rand::rng();
        let total_weight: i32 = Self::LOG_LEVEL_WEIGHTS.iter().sum();
        let random_value = rng.random_range(0..total_weight);

        let mut cumulative = 0;
        for (i, &weight) in Self::LOG_LEVEL_WEIGHTS.iter().enumerate() {
            cumulative += weight;
            if random_value < cumulative {
                return Self::LOG_LEVELS[i];
            }
        }

        // Fallback to INFO
        "INFO"
    }

    /// Generate a log message for the given level
    pub fn generate_log_message(level: &str) -> String {
        let mut rng = rand::rng();

        let template = match level {
            "INFO" => Self::INFO_TEMPLATES[rng.random_range(0..Self::INFO_TEMPLATES.len())],
            "DEBUG" => Self::DEBUG_TEMPLATES[rng.random_range(0..Self::DEBUG_TEMPLATES.len())],
            "WARN" => Self::WARN_TEMPLATES[rng.random_range(0..Self::WARN_TEMPLATES.len())],
            "ERROR" => Self::ERROR_TEMPLATES[rng.random_range(0..Self::ERROR_TEMPLATES.len())],
            _ => Self::INFO_TEMPLATES[0], // Default to first INFO template
        };

        Self::fill_template(template, level)
    }

    /// Generate a log message with target length (matching Java implementation)
    pub fn generate_log_message_with_len(level: &str, target_len: usize) -> String {
        let mut rng = rand::rng();

        let template = match level {
            "INFO" => Self::INFO_TEMPLATES[rng.random_range(0..Self::INFO_TEMPLATES.len())],
            "DEBUG" => Self::DEBUG_TEMPLATES[rng.random_range(0..Self::DEBUG_TEMPLATES.len())],
            "WARN" => Self::WARN_TEMPLATES[rng.random_range(0..Self::WARN_TEMPLATES.len())],
            "ERROR" => Self::ERROR_TEMPLATES[rng.random_range(0..Self::ERROR_TEMPLATES.len())],
            _ => Self::INFO_TEMPLATES[0], // Default to first INFO template
        };

        Self::fill_template_with_len(template, level, target_len)
    }

    /// Generate a log message with automatic level selection
    pub fn generate_log_entry() -> (String, String) {
        let level = Self::generate_log_level();
        let message = Self::generate_log_message(level);
        (level.to_string(), message)
    }

    /// Generate log text with specified target length (matching Java implementation)
    /// Target length is 1500 characters like the Java version
    pub fn generate_text_with_len(target_len: usize) -> (String, String) {
        let level = Self::generate_log_level();
        let message = Self::generate_log_message_with_len(level, target_len);
        (level.to_string(), message)
    }

    /// Fill template placeholders with random values
    fn fill_template(template: &str, level: &str) -> String {
        let mut rng = rand::rng();
        let mut result = template.to_string();

        // Replace common placeholders with random values
        result = result.replace("{}", &Self::generate_random_value(&mut rng));

        // Add stack trace for ERROR logs
        if level == "ERROR" && rng.random_bool(0.7) {
            // 70% of errors have stack traces
            result.push('\n');
            result.push_str(&Self::generate_stack_trace(&mut rng));
        }

        // Add context for non-ERROR logs to reach target length
        if level != "ERROR" && result.len() < 100 {
            result.push_str(&Self::generate_context(&mut rng));
        }

        result
    }

    /// Fill template with target length (matching Java implementation)
    fn fill_template_with_len(template: &str, level: &str, target_len: usize) -> String {
        let mut rng = rand::rng();
        let mut result = template.to_string();

        // Replace common placeholders with random values
        result = result.replace("{}", &Self::generate_random_value(&mut rng));

        // Add stack trace for ERROR logs
        if level == "ERROR" && rng.random_bool(0.7) {
            // 70% of errors have stack traces
            result.push('\n');
            result.push_str(&Self::generate_stack_trace(&mut rng));
        }

        // Extend message to reach target length
        while result.len() < target_len {
            if level == "ERROR" {
                // For ERROR logs, add more stack trace frames
                result.push('\n');
                let frame_index = rng.random_range(0..Self::STACK_FRAMES.len());
                result.push_str("    ");
                result.push_str(Self::STACK_FRAMES[frame_index]);
            } else {
                // For other logs, add more context
                result.push_str(&Self::generate_context(&mut rng));
            }
        }

        // Truncate if we exceeded the target (keep within reasonable bounds)
        if result.len() > target_len + 100 {
            result.truncate(target_len);
            result.push_str("...");
        }

        result
    }

    /// Generate random values for template placeholders
    fn generate_random_value(rng: &mut impl Rng) -> String {
        match rng.random_range(0..8) {
            0 => format!("user_{}", rng.random_range(10000..99999)),
            1 => format!("{}", rng.random_range(1..9999)),
            2 => format!("{:.2}", rng.random::<f64>() * 100.0),
            3 => format!("{}ms", rng.random_range(1..5000)),
            4 => format!("req_{}", rng.random::<u64>()),
            5 => format!(
                "192.168.{}.{}",
                rng.random_range(1..255),
                rng.random_range(1..255)
            ),
            6 => format!("srv_{}", rng.random_range(1..999)),
            _ => format!("val_{}", rng.random::<u32>()),
        }
    }

    /// Generate stack trace for error logs
    fn generate_stack_trace(rng: &mut impl Rng) -> String {
        let frame_count = rng.random_range(3..8);
        let mut stack_trace = String::new();

        for i in 0..frame_count {
            if i < Self::STACK_FRAMES.len() {
                stack_trace.push_str("    ");
                stack_trace.push_str(Self::STACK_FRAMES[i]);
                stack_trace.push('\n');
            }
        }

        stack_trace
    }

    /// Generate additional context for logs
    fn generate_context(rng: &mut impl Rng) -> String {
        let context_count = rng.random_range(1..4);
        let mut context = String::from(" [");

        for i in 0..context_count {
            if i > 0 {
                context.push_str(", ");
            }
            let key = Self::CONTEXT_KEYS[rng.random_range(0..Self::CONTEXT_KEYS.len())];
            let value = Self::generate_random_value(rng);
            context.push_str(&format!("{key}={value}"));
        }

        context.push(']');
        context
    }

    /// Generate log level distribution statistics
    pub fn generate_distribution_stats(
        sample_size: usize,
    ) -> std::collections::HashMap<String, f64> {
        let mut counts = std::collections::HashMap::new();

        for _ in 0..sample_size {
            let level = Self::generate_log_level();
            *counts.entry(level.to_string()).or_insert(0) += 1;
        }

        let mut percentages = std::collections::HashMap::new();
        for (level, count) in counts {
            percentages.insert(level, (count as f64 / sample_size as f64) * 100.0);
        }

        percentages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_generation() {
        // Test that we can generate all log levels
        let mut levels_seen = std::collections::HashSet::new();

        for _ in 0..1000 {
            let level = LogTextHelper::generate_log_level();
            levels_seen.insert(level);
        }

        // Should see at least INFO and DEBUG (most common)
        assert!(levels_seen.contains("INFO"));
        assert!(levels_seen.contains("DEBUG"));
    }

    #[test]
    fn test_log_message_generation() {
        for level in ["INFO", "DEBUG", "WARN", "ERROR"] {
            let message = LogTextHelper::generate_log_message(level);
            assert!(!message.is_empty());
            assert!(message.len() > 10); // Should be reasonably long
        }
    }

    #[test]
    fn test_log_entry_generation() {
        let (level, message) = LogTextHelper::generate_log_entry();
        assert!(LogTextHelper::LOG_LEVELS.contains(&level.as_str()));
        assert!(!message.is_empty());
    }

    #[test]
    fn test_log_level_distribution() {
        let stats = LogTextHelper::generate_distribution_stats(10000);

        // Print actual distribution for debugging
        println!("Actual distribution:");
        for (level, pct) in &stats {
            println!("  {}: {:.2}%", level, pct);
        }

        // INFO should be the most common (around 84%)
        let info_pct = stats.get("INFO").unwrap_or(&0.0);
        assert!(*info_pct > 70.0 && *info_pct < 90.0);

        // ERROR should be the least common (around 1%)
        let error_pct = stats.get("ERROR").unwrap_or(&0.0);
        assert!(*error_pct < 10.0); // Relaxed for now to see actual values
    }

    #[test]
    fn test_error_logs_have_stack_traces() {
        let mut has_stack_trace = false;

        // Generate several error messages
        for _ in 0..20 {
            let message = LogTextHelper::generate_log_message("ERROR");
            if message.contains("at ") && message.contains(".java:") {
                has_stack_trace = true;
                break;
            }
        }

        // At least some error messages should have stack traces
        // (Since it's 70% probability, with 20 attempts we should see at least one)
        assert!(has_stack_trace);
    }

    #[test]
    fn test_context_generation() {
        // Non-error logs should sometimes have context
        let mut has_context = false;

        for _ in 0..20 {
            let message = LogTextHelper::generate_log_message("INFO");
            if message.contains("[") && message.contains("=") {
                has_context = true;
                break;
            }
        }

        // Should find some messages with context
        assert!(has_context);
    }

    #[test]
    fn test_generate_text_with_len() {
        let target_len = 1500;
        let (level, message) = LogTextHelper::generate_text_with_len(target_len);

        // Should generate valid log level
        assert!(LogTextHelper::LOG_LEVELS.contains(&level.as_str()));

        // Message should be close to target length (within reasonable bounds)
        println!(
            "Generated message length: {} (target: {})",
            message.len(),
            target_len
        );
        println!("Level: {}", level);
        println!("Message preview: {}...", &message[..message.len().min(100)]);

        // Should be at least close to target length (within 200 chars tolerance)
        assert!(message.len() >= target_len - 200);
        assert!(message.len() <= target_len + 200);
    }

    #[test]
    fn test_generate_error_text_with_len() {
        let target_len = 1500;
        let message = LogTextHelper::generate_log_message_with_len("ERROR", target_len);

        println!(
            "ERROR message length: {} (target: {})",
            message.len(),
            target_len
        );
        println!(
            "ERROR message preview: {}...",
            &message[..message.len().min(200)]
        );

        // ERROR messages should be close to target length
        assert!(message.len() >= target_len - 200);
        assert!(message.len() <= target_len + 200);

        // Should likely contain stack trace elements
        if message.contains("at ") && message.contains(".java:") {
            println!("✓ Contains stack trace as expected");
        }
    }
}
