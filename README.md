# IPC Ring Buffer

A dual IPC ring buffer is 2 memory-mapped files. An endpoint opens its "owned" file as read-write and the shared file as read-only. The other endpoint opens the same files but with the inverse designation and permissions.

Both files have the same layout of the following structures, page-aligned:

```rust
/// Ring buffer part that is owned by the producer.
/// This gets mmapped into the producer as read-write
/// and into the consumer as read-only
#[repr(C)]
pub struct ProducerOwned<const N: usize> {
    write_ptr: Futex,
    read_ptr_contended: AtomicU32,
    buffer: UnsafeCell<[u8; N]>,
}

/// Ring buffer part that is owned by the consumer
/// This gets mmapped into the consumer as read-write
/// and into the producer as read-only
#[repr(C)]
pub struct ConsumerOwned<const N: usize> {
    read_ptr: Futex,
    write_ptr_contended: AtomicU32,
    buffer: UnsafeCell<[u8; N]>,
}
```
