//! Performance benchmarks for partition remapping.
//!
//! Measures latency and throughput of core remapping operations.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use kafka_remapper_core::config::MappingConfig;
use kafka_remapper_core::remapper::PartitionRemapper;

/// Create a remapper with the given configuration.
fn create_remapper(virtual_partitions: u32, physical_partitions: u32) -> PartitionRemapper {
    PartitionRemapper::new(&MappingConfig {
        virtual_partitions,
        physical_partitions,
        offset_range: 1 << 40,
        topics: Default::default(),
    })
}

/// Benchmark virtual to physical partition mapping.
fn bench_virtual_to_physical(c: &mut Criterion) {
    let mut group = c.benchmark_group("virtual_to_physical");

    for (virt_count, phys_count) in [(100, 10), (1000, 100), (10000, 100)] {
        let remapper = create_remapper(virt_count, phys_count);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("mapping", format!("{virt_count}:{phys_count}")),
            &remapper,
            |b, r| {
                b.iter(|| {
                    for v in 0..virt_count as i32 {
                        black_box(r.virtual_to_physical(v).unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark virtual to physical offset translation.
fn bench_offset_translation(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_translation");

    for (virt_count, phys_count) in [(100, 10), (1000, 100)] {
        let remapper = create_remapper(virt_count, phys_count);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("virtual_to_physical", format!("{virt_count}:{phys_count}")),
            &remapper,
            |b, r: &PartitionRemapper| {
                b.iter(|| {
                    black_box(r.virtual_to_physical_offset(50, 10000).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("physical_to_virtual", format!("{virt_count}:{phys_count}")),
            &remapper,
            |b, r: &PartitionRemapper| {
                // First get a valid physical offset
                let phys = r.virtual_to_physical_offset(50, 10000).unwrap();
                b.iter(|| {
                    black_box(
                        r.physical_to_virtual(phys.physical_partition, phys.physical_offset)
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark offset roundtrip (virtual -> physical -> virtual).
fn bench_offset_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_roundtrip");

    let remapper = create_remapper(100, 10);

    group.throughput(Throughput::Elements(1));
    group.bench_function("complete_roundtrip", |b| {
        b.iter(|| {
            let phys = remapper.virtual_to_physical_offset(42, 5000).unwrap();
            let back = remapper
                .physical_to_virtual(phys.physical_partition, phys.physical_offset)
                .unwrap();
            black_box(back);
        });
    });

    group.finish();
}

/// Benchmark getting offset range for virtual partition.
fn bench_get_offset_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_offset_range");

    let remapper = create_remapper(1000, 100);

    group.bench_function("single_partition", |b| {
        b.iter(|| {
            black_box(remapper.get_offset_range_for_virtual(500).unwrap());
        });
    });

    group.bench_function("all_partitions", |b| {
        b.iter(|| {
            for v in 0..1000 {
                black_box(remapper.get_offset_range_for_virtual(v).unwrap());
            }
        });
    });

    group.finish();
}

/// Benchmark offset belongs to virtual check.
fn bench_offset_belongs_to_virtual(c: &mut Criterion) {
    let mut group = c.benchmark_group("offset_belongs_to_virtual");

    let remapper = create_remapper(100, 10);

    // Get a valid physical offset for partition 42
    let phys = remapper.virtual_to_physical_offset(42, 5000).unwrap();

    group.bench_function("check", |b| {
        b.iter(|| {
            black_box(
                remapper
                    .offset_belongs_to_virtual(phys.physical_partition, phys.physical_offset, 42)
                    .unwrap(),
            );
        });
    });

    group.finish();
}

/// Benchmark virtual partitions for physical lookup.
fn bench_virtual_partitions_for_physical(c: &mut Criterion) {
    let mut group = c.benchmark_group("virtual_partitions_for_physical");

    for (virt_count, phys_count) in [(100, 10), (1000, 100), (10000, 100)] {
        let remapper = create_remapper(virt_count, phys_count);

        group.bench_with_input(
            BenchmarkId::new("lookup", format!("{virt_count}:{phys_count}")),
            &remapper,
            |b, r: &PartitionRemapper| {
                b.iter(|| {
                    black_box(r.virtual_partitions_for_physical(5));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_virtual_to_physical,
    bench_offset_translation,
    bench_offset_roundtrip,
    bench_get_offset_range,
    bench_offset_belongs_to_virtual,
    bench_virtual_partitions_for_physical,
);
criterion_main!(benches);
