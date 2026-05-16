use ring::{agreement, hkdf, rand};

use crate::ConnectionError;
use crate::ringbuffer_parts::ProducerOwned;
use crate::umask_context::UmaskContext;
use crate::{KEX_ALG, dual_ringbuffer::DualRingBuffers, owned_file_options, shared_file_options};
use std::fs::{Metadata, remove_file};
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{DirBuilder, remove_dir_all},
    io::{self, Read, Write},
    os::unix::fs::DirBuilderExt,
    path::Path,
};

pub struct Listener<P: AsRef<Path>, const N: usize> {
    my_dir: P,
    dual_ringbuffers: DualRingBuffers<N>,
    rng: rand::SystemRandom,
}

impl<P: AsRef<Path>, const N: usize> Listener<P, N> {
    pub fn new(dir: P) -> Result<Self, ConnectionError> {
        let _ctx = UmaskContext::new(0).expect("umask lock poisoned");
        let mut dir_builder = DirBuilder::new();
        dir_builder.mode(0o1733);

        if let Err(e) = dir_builder.create(&dir) {
            if let io::ErrorKind::AlreadyExists = e.kind() {
                remove_dir_all(&dir).map_err(ConnectionError::Dir)?;
                dir_builder.create(&dir).map_err(ConnectionError::Dir)?;
            } else {
                return Err(ConnectionError::Dir(e));
            }
        }

        let owned_path = dir.as_ref().join("server");
        let shared_path = dir.as_ref().join("client");

        let owned_file = owned_file_options()
            .open(&owned_path)
            .map_err(ConnectionError::Io)?;

        let shared_file = shared_file_options()
            .write(true)
            .create_new(true)
            .mode(0o666)
            .open(&shared_path)
            .map_err(ConnectionError::Io)?;

        shared_file
            .set_len(ProducerOwned::<N>::page_aligned_size() as u64 * 2)
            .map_err(ConnectionError::Io)?;

        let dual_ringbuffers = DualRingBuffers::new_server(owned_file, shared_file)
            .map_err(ConnectionError::RingBufferError)?;

        Ok(Self {
            my_dir: dir,
            dual_ringbuffers,
            rng: rand::SystemRandom::new(),
        })
    }

    pub fn set_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.dual_ringbuffers.set_timeout(timeout);
    }

    pub fn accept<F: FnOnce(&Metadata) -> bool>(
        &mut self,
        accept_fn: F,
    ) -> Result<(Metadata, DualRingBuffers<N>), ConnectionError> {
        // TODO: add a timeout
        let mut peer_public_key_buf = [0u8; 65];
        self.dual_ringbuffers
            .read_exact(&mut peer_public_key_buf)
            .map_err(ConnectionError::Io)?;

        // send our public key and derive the shared secret, which will be used as a prefix for the secret file paths
        let secret_file_prefix = self.key_agreement(&peer_public_key_buf)?;

        // wait for client ready signal
        // sent when the client creates the shared file
        // at <my_dir>/<secret_file_prefix>-client
        let mut ready_signal = [0u8; 1];
        self.dual_ringbuffers
            .read_exact(&mut ready_signal)
            .map_err(ConnectionError::Io)?;

        if ready_signal[0] != 1 {
            return Err(ConnectionError::ClientError);
        }

        let secret_file_prefix = hex::encode(&secret_file_prefix);

        // open the file, check its ownership and decide whether to accept the connection
        let mut secret_client_name = secret_file_prefix.clone();
        secret_client_name.push_str("-client");
        let secret_client_path = self.my_dir.as_ref().join(secret_client_name);

        let shared_file = shared_file_options()
            .open(&secret_client_path)
            .map_err(ConnectionError::Io)?;

        let client_file_metadata = shared_file.metadata().map_err(|e| ConnectionError::Io(e))?;
        if !accept_fn(&client_file_metadata) {
            self.dual_ringbuffers
                .write_all(&[0])
                .map_err(ConnectionError::Io)?;

            return Err(ConnectionError::PeerRejected(client_file_metadata));
        }

        let mut secret_server_name = secret_file_prefix.clone();
        secret_server_name.push_str("-server");
        let secret_server_path = self.my_dir.as_ref().join(secret_server_name);
        let owned_file = owned_file_options()
            .open(&secret_server_path)
            .map_err(ConnectionError::Io)?;

        let mut new_connection = DualRingBuffers::<N>::new_server(owned_file, shared_file)
            .map_err(ConnectionError::RingBufferError)?;

        self.dual_ringbuffers
            .write_all(&[1])
            .map_err(ConnectionError::Io)?;

        new_connection
            .read_exact(&mut ready_signal)
            .map_err(ConnectionError::Io)?;

        if ready_signal[0] != 1 {
            return Err(ConnectionError::ClientError);
        }

        remove_file(secret_server_path).map_err(ConnectionError::Io)?;

        Ok((client_file_metadata, new_connection))
    }

    fn key_agreement(&mut self, peer_public_key: &[u8; 65]) -> Result<[u8; 32], ConnectionError> {
        let peer_public_key = agreement::UnparsedPublicKey::new(KEX_ALG, peer_public_key);
        let my_private_key = agreement::EphemeralPrivateKey::generate(KEX_ALG, &self.rng)
            .expect("failed to generate ephemeral private key");

        let my_public_key = my_private_key
            .compute_public_key()
            .expect("failed to compute my public key");

        self.dual_ringbuffers
            .write_all(my_public_key.as_ref())
            .map_err(ConnectionError::Io)?;

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

impl<P: AsRef<Path>, const N: usize> Drop for Listener<P, N> {
    fn drop(&mut self) {
        let _ = remove_dir_all(&self.my_dir);
    }
}
