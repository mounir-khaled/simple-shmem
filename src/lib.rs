#![allow(dead_code)]

mod dual_ringbuffer;
mod futex;
mod page_size;
mod producer_consumer;
mod ringbuffer_parts;
mod umask_context;

mod listener;

use std::{
    fs::{Metadata, OpenOptions},
    io::{self},
    os::unix::fs::OpenOptionsExt,
};

use ring::agreement;
use thiserror::Error;

pub use crate::dual_ringbuffer::{DualRingBuffers, DualRingBuffersError};
pub use crate::listener::Listener;

// 64-byte cache-line header + 4032-byte buffer = exactly one 4096-byte page
pub type StdListener<P> = Listener<P, 4032>;
pub type StdDualRingBuffers = DualRingBuffers<4032>;

const KEX_ALG: &agreement::Algorithm = &agreement::ECDH_P256;

fn owned_file_options() -> OpenOptions {
    let mut oo = OpenOptions::new();

    oo.read(true)
        .write(true)
        .truncate(true)
        .create_new(true)
        .mode(0o644);

    oo
}

fn shared_file_options() -> OpenOptions {
    let mut oo = OpenOptions::new();
    oo.read(true).custom_flags(libc::O_NOFOLLOW);

    oo
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Directory error: {0}")]
    Dir(io::Error),
    #[error("Key agreement error: {0}")]
    KeyAgreement(ring::error::Unspecified),
    #[error("Ring buffer error: {0}")]
    RingBufferError(DualRingBuffersError),
    #[error("I/O error: {0}")]
    Io(io::Error),
    #[error("Peer rejected: {0:?}")]
    PeerRejected(Metadata),
    #[error("Client error")]
    ClientError,
}
