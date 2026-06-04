# Shared Memory IPC in Rust

Establishing shared memory for interprocess communication (IPC) between mutually distrustful processes is non-trivial to implement safely. This library aims to do that securely, using an already-established Unix domain socket connection to share the shared memory file descriptors, and modifying their seals to protect them from tampering by the other endpoint. 

### Quick Example
#### Server:
```
let listener = UnixListener::bind("/tmp/ping.sock")?;
let (stream, _) = listener.accept()?;
let mut ring_buffer = FastDualRingBuffers::accept(&stream)?;
ring_buffer.write_all(b"ping")?;
```
#### Client:
```
let (stream, _) = UnixStream::connect("/tmp/ping.sock")?;
let mut ring_buffer = FastDualRingBuffers::connect(&stream)?;
let mut buf = [0u8; 4];
ring_buffer.read_exact(&mut buf)?;
assert_eq!(&buf, b"ping");
```

See `examples/` for more examples.

### Dual Ring Buffer Layout

A dual IPC ring buffer is 2 memory-mapped memfds. An endpoint mmaps its "owned" memfd as read-write and the shared memfd as read-only. The other endpoint opens the same memfds but with the inverse designation and permissions.

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

### Why read/write API instead of zerocopy?

Time-of-check-time-of-use (TOCTOU) vulnerabilities; With zerocopy, a malicious endpoint can send some benign data, the victim process validates the data to be "secure", then the malicious endpoint later modifies the data to be malicious after it passes validation but before it gets used.
