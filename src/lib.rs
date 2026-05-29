#![allow(dead_code)]

mod dual_ringbuffer;
mod futex;
mod page_size;
mod peercred;
mod producer_consumer;
mod ringbuffer_parts;

pub mod connect;

pub use crate::dual_ringbuffer::DualRingBuffers;

// exactly one 4096-byte page
pub type StdDualRingBuffers = DualRingBuffers<4088>;

// 8-byte header + 32-byte buffer = 40 bytes, fits in one 64-byte cache line.
// write_ptr, read_ptr_contended, and the entire buffer share one cache line,
// so a single L3 transfer delivers both the "data ready" signal and the payload.
// N=32 (power of 2) allows the ring pointer modulo to compile to a single AND
// instruction instead of a multiply-shift sequence.
pub type FastDualRingBuffers = DualRingBuffers<32>;
