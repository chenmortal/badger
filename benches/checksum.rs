#[allow(unused)]
use std::mem::replace;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;

pub(crate) fn generate_data(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    getrandom::getrandom(&mut buf).unwrap();
    buf
}
fn checksum_with_update(data: &Vec<u8>, block_size: usize, hash: u32) {
    let mut hasher = crc32fast::Hasher::new();
    // let data = generate_data(size);
    let mut buf = Vec::with_capacity(block_size);
    let mut result = Vec::new();
    for ele in data {
        if buf.len() == buf.capacity() {
            hasher.update(buf.as_ref());
            result.extend_from_slice(&buf);
            buf.clear();
        }
        buf.push(*ele);
    }
    hasher.update(buf.as_ref());
    assert_eq!(hasher.finalize(), hash);
    // let _ = hasher.finalize();
}
fn checksum_without_update(data: &Vec<u8>, block_size: usize, hash: u32) {
    let mut buf = Vec::with_capacity(block_size);
    let mut result = Vec::new();
    for ele in data {
        if buf.len() == buf.capacity() {
            result.extend_from_slice(&buf);
            buf.clear();
        }
        buf.push(*ele);
    }
    result.extend_from_slice(buf.as_ref());
    assert_eq!(crc32fast::hash(&result), hash)
}
fn bench_checksum(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32");
    static KB: usize = 1024;
    static MB: usize = 1024 * KB;
    for (size, block_size) in [
        (16 * MB, 4 * KB),
        (16 * MB, 8 * KB),
        (32 * MB, 4 * KB),
        (32 * MB, 8 * KB),
        (64 * MB, 4 * KB),
        (64 * MB, 8 * KB),
    ]
    .iter()
    {
        let data = generate_data(*size);
        let hash = crc32fast::hash(&data);
        group.throughput(Throughput::Bytes(*size as u64));
        let par =
            (size / MB).to_string() + "MB" + "-" + (block_size / KB).to_string().as_str() + "KB";
        group.bench_with_input(
            BenchmarkId::new("with_update", par.clone()),
            &data,
            |b, data| {
                b.iter(|| checksum_with_update(data, *block_size, hash));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("without_update", par.clone()),
            &data,
            |b, data| {
                b.iter(|| checksum_without_update(data, *block_size, hash));
            },
        );
    }
    group.finish();
}
criterion_group!(benches, bench_checksum);
criterion_main!(benches);
