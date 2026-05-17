use std::{
    fs::{File, remove_file},
    io::{self, Read, Write},
    os::fd::AsFd,
    path::Path,
    time::Duration,
};

use ring::{agreement, hkdf};
use thiserror::Error;

use crate::{
    ConnectionError, KEX_ALG, owned_file_options,
    producer_consumer::{Consumer, ConsumerProducerError, Producer},
    ringbuffer_parts::ProducerOwned,
    shared_file_options,
};

// TODO: implement Drop and delete owned file
pub struct DualRingBuffers<const N: usize> {
    consumer: Consumer<N>,
    producer: Producer<N>,
}

#[derive(Error, Debug)]
pub enum DualRingBuffersError {
    #[error("Consumer error: {0}")]
    Consumer(ConsumerProducerError),
    #[error("Producer error: {0}")]
    Producer(ConsumerProducerError),
    #[error("Owned file error: {0}")]
    OwnedFile(io::Error),
    #[error("Shared file error: {0}")]
    SharedFile(io::Error),
    #[error("Directory error: {0}")]
    Dir(io::Error),
}

impl<const N: usize> DualRingBuffers<N> {
    /// Connect using `HANDSHAKE_N` for the handshake channel and `N` (Self) for the data channel.
    /// Use this when the listener was created with a different buffer size for the handshake
    /// than the desired data-channel buffer size (e.g. `HANDSHAKE_N = 4032`, `N = 56`).
    pub fn connect<const HANDSHAKE_N: usize, P: AsRef<Path>>(
        dir: P,
    ) -> Result<DualRingBuffers<N>, ConnectionError> {
        let owned_path = dir.as_ref().join("client");
        let shared_path = dir.as_ref().join("server");

        let owned_file = owned_file_options()
            .create_new(false)
            .truncate(false)
            .open(&owned_path)
            .map_err(ConnectionError::Io)?;

        let conn_shared_file = shared_file_options()
            .open(&shared_path)
            .map_err(ConnectionError::Io)?;

        let mut conn_rb = DualRingBuffers::<HANDSHAKE_N>::new_client(owned_file, conn_shared_file)
            .map_err(ConnectionError::RingBufferError)?;

        let secret_file_prefix = conn_rb.key_agreement()?;
        let secret_file_prefix = hex::encode(secret_file_prefix);
        let mut owned_file_name = secret_file_prefix.clone();
        owned_file_name.push_str("-client");
        let owned_file_path = dir.as_ref().join(owned_file_name);

        let owned_file = owned_file_options()
            .open(&owned_file_path)
            .map_err(ConnectionError::Io)?;

        // Resize before signaling ready so the server can safely mmap this file
        owned_file
            .set_len(ProducerOwned::<N>::page_aligned_size() as u64 * 2)
            .map_err(ConnectionError::Io)?;

        conn_rb.write_all(&[1]).map_err(ConnectionError::Io)?;

        let mut server_ready = [0u8; 1];
        conn_rb
            .read_exact(&mut server_ready)
            .map_err(ConnectionError::Io)?;

        if server_ready[0] != 1 {
            return Err(ConnectionError::Io(io::Error::new(
                io::ErrorKind::Other,
                "server failed to create owned file",
            )));
        }

        remove_file(owned_file_path).map_err(ConnectionError::Io)?;

        let mut secret_server_name = secret_file_prefix.clone();
        secret_server_name.push_str("-server");
        let secret_server_path = dir.as_ref().join(secret_server_name);
        let shared_file = shared_file_options()
            .open(&secret_server_path)
            .map_err(ConnectionError::Io)?;

        let mut new_connection = DualRingBuffers::<N>::new_client(owned_file, shared_file)
            .map_err(ConnectionError::RingBufferError)?;

        new_connection
            .write_all(&[1])
            .map_err(ConnectionError::Io)?;

        Ok(new_connection)
    }

    pub(crate) fn new_server(
        owned_file: File,
        shared_file: File,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();

        owned_file
            .set_len(page_aligned_buffer_size as u64 * 2)
            .map_err(DualRingBuffersError::OwnedFile)?;

        let consumer = Consumer::new(owned_file.as_fd(), 0, shared_file.as_fd(), 0)
            .map_err(DualRingBuffersError::Consumer)?;

        let producer = Producer::new(
            owned_file.as_fd(),
            page_aligned_buffer_size,
            shared_file.as_fd(),
            page_aligned_buffer_size,
        )
        .map_err(DualRingBuffersError::Producer)?;

        Ok(DualRingBuffers { consumer, producer })
    }

    pub(crate) fn new_client(
        owned_file: File,
        shared_file: File,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();

        owned_file
            .set_len(page_aligned_buffer_size as u64 * 2)
            .map_err(DualRingBuffersError::OwnedFile)?;

        let consumer = Consumer::new(
            owned_file.as_fd(),
            page_aligned_buffer_size,
            shared_file.as_fd(),
            page_aligned_buffer_size,
        )
        .map_err(DualRingBuffersError::Consumer)?;

        let producer = Producer::new(owned_file.as_fd(), 0, shared_file.as_fd(), 0)
            .map_err(DualRingBuffersError::Producer)?;

        Ok(DualRingBuffers { consumer, producer })
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.consumer.set_timeout(timeout);
        self.producer.set_timeout(timeout);
    }

    pub fn set_spin_limit(&mut self, spin_limit: u32) {
        self.consumer.set_spin_limit(spin_limit);
        self.producer.set_spin_limit(spin_limit);
    }

    fn key_agreement(&mut self) -> Result<[u8; 32], ConnectionError> {
        let rng = ring::rand::SystemRandom::new();
        let my_private_key = agreement::EphemeralPrivateKey::generate(KEX_ALG, &rng)
            .expect("failed to generate ephemeral private key");

        let my_public_key = my_private_key
            .compute_public_key()
            .expect("failed to compute my public key");

        self.write_all(my_public_key.as_ref())
            .map_err(ConnectionError::Io)?;

        let mut peer_public_key = [0u8; 65];
        self.read_exact(&mut peer_public_key)
            .map_err(ConnectionError::Io)?;

        let peer_public_key = agreement::UnparsedPublicKey::new(KEX_ALG, peer_public_key);

        let shared_secret =
            agreement::agree_ephemeral(my_private_key, &peer_public_key, |key_material| {
                let mut shared_secret = [0u8; 32];

                let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, &[1, 2, 3, 4]);
                let prk = salt.extract(key_material);

                let okm = prk
                    .expand(&[], &ring::aead::AES_256_GCM)
                    .expect("failed to expand shared secret");

                okm.fill(&mut shared_secret)
                    .expect("failed to fill shared secret");

                shared_secret
            })
            .map_err(ConnectionError::KeyAgreement)?;

        Ok(shared_secret)
    }
}

impl<const N: usize> io::Read for DualRingBuffers<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.consumer.read(buf)
    }
}

impl<const N: usize> io::Write for DualRingBuffers<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.producer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.producer.flush()
    }
}

impl<const N: usize> DualRingBuffers<N> {
    /// Read exactly `LEN` bytes with a compile-time-known size so LLVM can
    /// inline the copy as a direct load/store instead of a `memcpy` call.
    pub fn read_fixed<const LEN: usize>(&mut self, buf: &mut [u8; LEN]) -> io::Result<()> {
        self.consumer.read_fixed(buf)
    }

    /// Write exactly `LEN` bytes with a compile-time-known size so LLVM can
    /// inline the copy as a direct load/store instead of a `memcpy` call.
    pub fn write_fixed<const LEN: usize>(&mut self, buf: &[u8; LEN]) -> io::Result<()> {
        self.producer.write_fixed(buf)
    }
}
