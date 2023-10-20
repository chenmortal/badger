use std::mem::replace;
use std::time::Duration;

use async_channel::Receiver;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use rayon::prelude::IntoParallelRefIterator;
use rayon::prelude::ParallelIterator;
use rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

fn generate_data(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    getrandom::getrandom(&mut buf).unwrap();
    buf
}
#[derive(Debug, Clone, Copy)]
enum CompressType {
    None,
    Snap,
    Zstd(i32),
}
impl CompressType {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self {
            CompressType::None => data.to_vec(),
            CompressType::Snap => snap::raw::Encoder::new().compress_vec(&data).unwrap(),
            CompressType::Zstd(level) => zstd::encode_all(data, *level).unwrap(),
        }
    }
}
fn sync_with_rayon(data: &Vec<u8>, block_size: usize, compress: CompressType) {
    let mut data_vec = Vec::new();
    let mut buf = Vec::with_capacity(block_size);
    for ele in data {
        if buf.len() < block_size {
            buf.push(*ele);
        } else {
            let old = replace(&mut buf, Vec::with_capacity(block_size));
            data_vec.push(old);
        }
    }
    let _ = data_vec
        .par_iter()
        .map(|x| compress.compress(x))
        .collect::<Vec<_>>();
}
fn stupid(data: &Vec<u8>, block_size: usize, compress: CompressType) {
    let mut data_vec = Vec::new();
    let mut buf = Vec::with_capacity(block_size);
    for ele in data {
        if buf.len() < block_size {
            buf.push(*ele);
        } else {
            let old = replace(&mut buf, Vec::with_capacity(block_size));
            data_vec.push(old);
        }
    }
    let p = data_vec
        .iter()
        .map(|x| compress.compress(x))
        .collect::<Vec<_>>();
    let k: usize = p.iter().map(|x| x.len()).sum();
}
async fn async_with_channel(data: &Vec<u8>, block_size: usize, compress: CompressType) {
    // let mut data_vec = Vec::new();
    let mut buf = Vec::with_capacity(block_size);
    let (send, recv) = async_channel::bounded::<Vec<u8>>(1000);
    let (send_back, recv_back) = tokio::sync::mpsc::channel::<Vec<u8>>(1000);

    let compress_task = (0..16)
        .map(|_| {
            let recv_clone = recv.clone();
            let send_clone = send_back.clone();
            tokio::spawn(handle_compress(recv_clone, send_clone, compress))
        })
        .collect::<Vec<_>>();
    let collect = tokio::spawn(collect_compress(recv_back));
    for ele in data {
        if buf.len() < block_size {
            buf.push(*ele);
        } else {
            let old = replace(&mut buf, Vec::with_capacity(block_size));
            let _ = send.send(old).await;
        }
    }
    drop(send);
    for ele in compress_task {
        let _ = ele.await;
    }
    drop(send_back);
    let s = collect.await.unwrap();
}
async fn handle_compress(
    recv: Receiver<Vec<u8>>,
    send: tokio::sync::mpsc::Sender<Vec<u8>>,
    compress: CompressType,
) {
    while let Ok(s) = recv.recv().await {
        let _ = send.send(compress.compress(&s)).await;
    }
}
async fn collect_compress(mut recv: tokio::sync::mpsc::Receiver<Vec<u8>>) -> Vec<Vec<u8>> {
    let mut res = Vec::new();
    while let Some(s) = recv.recv().await {
        res.push(s);
    }
    return res;
}
async fn async_with_rayon_pool(
    pool: &ThreadPool,
    data: &Vec<u8>,
    block_size: usize,
    compress: CompressType,
) {
    let mut buf = Vec::with_capacity(block_size);
    let mut task_v = Vec::new();
    for ele in data {
        if buf.len() < block_size {
            buf.push(*ele);
        } else {
            let old = replace(&mut buf, Vec::with_capacity(block_size));
            let k = pool.spawn_async(move || compress.compress(&old));
            task_v.push(k);
        }
    }
    let mut data_vec = Vec::new();
    for ele in task_v {
        data_vec.push(ele.await);
    }
}
fn bench_compress(c: &mut Criterion, group_name: &str, compress: CompressType) {
    let mut group = c.benchmark_group(group_name);
    group.warm_up_time(Duration::from_secs(7));
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(16)
            .build()
            .unwrap();
        group.throughput(Throughput::Bytes(*size as u64));
        group.warm_up_time(Duration::from_secs(3));
        let par =
            (size / MB).to_string() + "MB" + "-" + (block_size / KB).to_string().as_str() + "KB";
        group.bench_with_input(BenchmarkId::new("stupid", par.clone()), &data, |b, data| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { stupid(data, *block_size, compress) })
            // b.iter(|| stupid(data, *block_size, compress))
        });
        group.bench_with_input(
            BenchmarkId::new("sync_with_rayon", par.clone()),
            &data,
            |b, data| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async { sync_with_rayon(data, *block_size, compress) })
                // b.iter(|| sync_with_rayon(data, *block_size, compress))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("async_with_channel", par.clone()),
            &data,
            |b, data| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async_with_channel(data, *block_size, compress))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("async_with_rayon_pool", par),
            &data,
            |b, data| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async_with_rayon_pool(&pool, data, *block_size, compress));
            },
        );
    }
    group.finish();
}
fn bench_none(c: &mut Criterion) {
    bench_compress(c, "none", CompressType::None)
}
fn bench_snap(c: &mut Criterion) {
    bench_compress(c, "snap", CompressType::Snap)
}
fn bench_zstd1(c: &mut Criterion) {
    bench_compress(c, "zstd_1", CompressType::Zstd(1))
}
fn bench_zstd3(c: &mut Criterion) {
    bench_compress(c, "zstd_3", CompressType::Zstd(3))
}

// criterion_group!(benches, bench_fibs);
// criterion_group!(benches, bench_snap);
// criterion_group!(benches, bench_none);
criterion_group!(benches, bench_zstd3);
criterion_main!(benches);
// #[inline]
// fn fibonacci(n: u64) -> u64 {
//     match n {
//         0 => 1,
//         1 => 1,
//         n => fibonacci(n - 1) + fibonacci(n - 2),
//     }
// }
// use criterion::{black_box, criterion_group, criterion_main, Criterion};
// // use mycrate::fibonacci;

// pub fn criterion_benchmark(c: &mut Criterion) {
//     c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
// }

// criterion_group!(benches, criterion_benchmark);
// criterion_main!(benches);
