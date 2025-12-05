use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ferrokv::{FerroKv, WriteBatch};
use tempfile::TempDir;
use tokio::runtime::{self, Runtime};

const VALUE_SIZE: usize = 1024;

fn create_runtime() -> Runtime {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
}

fn generate_value() -> Vec<u8> {
    vec![b'x'; VALUE_SIZE]
}

fn generate_key(prefix: &str, counter: u64) -> Vec<u8> {
    format!("{prefix}:{counter:08}").into_bytes()
}

fn write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");
    group.throughput(Throughput::Bytes(VALUE_SIZE as u64));
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    let rt = create_runtime();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db =
        rt.block_on(async { FerroKv::open(temp_dir.path()).await.expect("Failed to open db") });
    let db = Arc::new(db);
    let value = generate_value();

    // Benchmark: set (durable write with fsync)
    let counter = AtomicU64::new(0);
    group.bench_function("set_durable", |b| {
        b.to_async(&rt).iter(|| {
            let db = Arc::clone(&db);
            let value = value.clone();
            let key_id = counter.fetch_add(1, Ordering::Relaxed);
            async move {
                let key = generate_key("bench", key_id);
                db.set(&key, &value).await.expect("set failed");
            }
        });
    });

    group.finish();

    // Cleanup
    drop(db);
    drop(temp_dir);
}

fn read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_throughput");
    group.throughput(Throughput::Elements(1));
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    let rt = create_runtime();

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db = rt.block_on(async {
        let db = FerroKv::open(temp_dir.path()).await.expect("Failed to open db");
        let value = generate_value();

        // Insert keys that will be in memtable
        for i in 0..100 {
            let key = generate_key("memtable", i);
            db.set(&key, &value).await.expect("set failed");
        }

        db
    });
    let db = Arc::new(db);

    // Benchmark: get from memtable (hot path)
    {
        let db = Arc::clone(&db);
        let key = generate_key("memtable", 50);

        group.bench_function("get_memtable_hit", |b| {
            b.to_async(&rt).iter(|| {
                let db = Arc::clone(&db);
                let key = key.clone();
                async move {
                    let result = db.get(&key).await.expect("get failed");
                    black_box(result)
                }
            });
        });
    }

    // Benchmark: get miss (key doesn't exist)
    {
        let db = Arc::clone(&db);
        let key = generate_key("nonexistent", 999_999);

        group.bench_function("get_miss", |b| {
            b.to_async(&rt).iter(|| {
                let db = Arc::clone(&db);
                let key = key.clone();
                async move {
                    let result = db.get(&key).await.expect("get failed");
                    black_box(result)
                }
            });
        });
    }

    group.finish();

    // Cleanup
    drop(db);
    drop(temp_dir);
}

fn ttl_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_overhead");
    group.throughput(Throughput::Bytes(VALUE_SIZE as u64));
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    let rt = create_runtime();
    let value = generate_value();

    // Benchmark: set without TTL (baseline)
    {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db =
            rt.block_on(async { FerroKv::open(temp_dir.path()).await.expect("Failed to open db") });
        let db = Arc::new(db);
        let counter = AtomicU64::new(0);
        let value = value.clone();

        group.bench_function("set_no_ttl", |b| {
            b.to_async(&rt).iter(|| {
                let db = Arc::clone(&db);
                let value = value.clone();
                let key_id = counter.fetch_add(1, Ordering::Relaxed);
                async move {
                    let key = generate_key("ttl", key_id);
                    db.set(&key, &value).await.expect("set failed");
                }
            });
        });

        drop(db);
        drop(temp_dir);
    }

    // Benchmark: set_ex with TTL (measure overhead)
    {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let db =
            rt.block_on(async { FerroKv::open(temp_dir.path()).await.expect("Failed to open db") });
        let db = Arc::new(db);
        let counter = AtomicU64::new(0);
        let value = value.clone();

        group.bench_function("set_with_ttl", |b| {
            b.to_async(&rt).iter(|| {
                let db = Arc::clone(&db);
                let value = value.clone();
                let key_id = counter.fetch_add(1, Ordering::Relaxed);
                async move {
                    let key = generate_key("ttl", key_id);
                    db.set_ex(&key, &value, Duration::from_secs(3600))
                        .await
                        .expect("set_ex failed");
                }
            });
        });

        drop(db);
        drop(temp_dir);
    }

    group.finish();
}

fn batch_vs_individual(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_vs_individual");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    let rt = create_runtime();
    let value = generate_value();

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size));

        // Benchmark: N individual writes (N fsyncs)
        {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let db = rt.block_on(async {
                FerroKv::open(temp_dir.path()).await.expect("Failed to open db")
            });
            let db = Arc::new(db);
            let value = value.clone();

            group.bench_with_input(
                BenchmarkId::new("individual_writes", batch_size),
                &batch_size,
                |b, &size| {
                    b.to_async(&rt).iter(|| {
                        let db = Arc::clone(&db);
                        let value = value.clone();
                        async move {
                            for i in 0..size {
                                let key = generate_key("individual", i);
                                db.set(&key, &value).await.expect("set failed");
                            }
                        }
                    });
                },
            );

            drop(db);
            drop(temp_dir);
        }

        // Benchmark: Single batch write (1 fsync for N operations)
        {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let db = rt.block_on(async {
                FerroKv::open(temp_dir.path()).await.expect("Failed to open db")
            });
            let db = Arc::new(db);
            let value = value.clone();

            group.bench_with_input(
                BenchmarkId::new("batched_writes", batch_size),
                &batch_size,
                |b, &size| {
                    b.to_async(&rt).iter(|| {
                        let db = Arc::clone(&db);
                        let value = value.clone();
                        async move {
                            let mut batch = WriteBatch::new();
                            for i in 0..size {
                                let key = generate_key("batched", i);
                                batch.set(&key, &value);
                            }
                            db.write_batch(batch).await.expect("write_batch failed");
                        }
                    });
                },
            );

            drop(db);
            drop(temp_dir);
        }
    }

    group.finish();
}

criterion_group!(benches, write_throughput, read_throughput, ttl_overhead, batch_vs_individual,);

criterion_main!(benches);
