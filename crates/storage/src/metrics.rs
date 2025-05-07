//! Storage metrics.
//!
//! The constants defined in this module are the names of metrics that the
//! backends maintain via [`metrics`] crate interfaces.

use ::metrics::{describe_counter, describe_histogram, Unit as MetricUnit};

/// Total number of files created.
pub const FILES_CREATED: &str = "disk.total_files_created";

/// Total number of files deleted.
pub const FILES_DELETED: &str = "disk.total_files_deleted";

/// Total number of successful disk writes.
pub const WRITES_SUCCESS: &str = "disk.total_writes_success";

/// Total number of failed disk writes.
pub const WRITES_FAILED: &str = "disk.total_writes_failed";

/// Total number of successful disk reads.
pub const READS_SUCCESS: &str = "disk.total_reads_success";

/// Total number of failed disk reads.
pub const READS_FAILED: &str = "disk.total_reads_failed";

/// Total number of bytes successfully written.
pub const TOTAL_BYTES_WRITTEN: &str = "disk.total_bytes_written";

/// Total number of bytes successfully read.
pub const TOTAL_BYTES_READ: &str = "disk.total_bytes_read";

/// Histogram of read latency.
pub const READ_LATENCY: &str = "disk.read_latency";

/// Histogram of write latency.
pub const WRITE_LATENCY: &str = "disk.write_latency";

pub fn describe_metrics() {
    describe_counter!(FILES_CREATED, "total number of files created");
    describe_counter!(FILES_DELETED, "total number of files deleted");
    describe_counter!(WRITES_SUCCESS, "total number of disk writes");
    describe_counter!(WRITES_FAILED, "total number of failed writes");
    describe_counter!(READS_SUCCESS, "total number of disk reads");
    describe_counter!(READS_FAILED, "total number of failed reads");

    describe_counter!(
        TOTAL_BYTES_WRITTEN,
        MetricUnit::Bytes,
        "total number of bytes written to disk"
    );
    describe_counter!(
        TOTAL_BYTES_READ,
        MetricUnit::Bytes,
        "total number of bytes read from disk"
    );

    describe_histogram!(READ_LATENCY, MetricUnit::Seconds, "Read request latency");
    describe_histogram!(WRITE_LATENCY, MetricUnit::Seconds, "Write request latency");
}
