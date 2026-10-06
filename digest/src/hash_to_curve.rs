//! RFC 9380 `hash_to_ristretto255`: the random-oracle hash from byte strings
//! into ristretto255 that leaf points are made with.

use curve25519_dalek::ristretto::RistrettoPoint;
use sha2::{Digest as _, Sha512};

/// SHA-512's output size in bytes, `b_in_bytes` in RFC 9380 section 5.3.1.
const B_IN_BYTES: usize = 64;

/// SHA-512's input block size in bytes, `s_in_bytes` in RFC 9380 section 5.3.1.
const S_IN_BYTES: usize = 128;

/// Hash `msg` under the domain separation tag `dst` to a ristretto255 element
/// with RFC 9380's `hash_to_ristretto255` (appendix B): expand_message_xmd with
/// SHA-512 (section 5.3.1) stretches `msg` to 64 uniform bytes, and the RFC 9496
/// element derivation (section 4.3.4, `RistrettoPoint::from_uniform_bytes`)
/// maps them to the group.
pub(crate) fn hash_to_ristretto255(msg: &[u8], dst: &[u8]) -> RistrettoPoint {
    let mut uniform_bytes = [0u8; 64];
    expand_message_xmd_sha512(msg, dst, &mut uniform_bytes);
    RistrettoPoint::from_uniform_bytes(&uniform_bytes)
}

/// RFC 9380 section 5.3.1 expand_message_xmd with SHA-512, filling `out`
/// (whose length is `len_in_bytes`).
///
/// # Panics
///
/// If `dst` is longer than 255 bytes, which RFC 9380 section 5.3.3 would hash
/// down first and no tag here needs, or if `out` is longer than the 255 blocks
/// the expansion can produce.
fn expand_message_xmd_sha512(msg: &[u8], dst: &[u8], out: &mut [u8]) {
    let dst_len = u8::try_from(dst.len()).expect("domain separation tags are at most 255 bytes");
    assert!(out.len().div_ceil(B_IN_BYTES) <= 255, "expand_message_xmd produces at most 255 blocks");
    // 255 blocks of 64 bytes fit the two bytes RFC 9380 gives the length.
    let len_in_bytes = out.len() as u16;
    // DST_prime = DST || I2OSP(len(DST), 1)
    let dst_prime = |hasher: &mut Sha512| {
        hasher.update(dst);
        hasher.update([dst_len]);
    };

    // b_0 = H(Z_pad || msg || l_i_b_str || I2OSP(0, 1) || DST_prime)
    let mut hasher = Sha512::new();
    hasher.update([0u8; S_IN_BYTES]);
    hasher.update(msg);
    hasher.update(len_in_bytes.to_be_bytes());
    hasher.update([0u8]);
    dst_prime(&mut hasher);
    let b_0: [u8; B_IN_BYTES] = hasher.finalize().into();

    // b_1 = H(b_0 || I2OSP(1, 1) || DST_prime), and for i > 1
    // b_i = H(strxor(b_0, b_(i-1)) || I2OSP(i, 1) || DST_prime);
    // the output is b_1 || b_2 || ..., truncated to len_in_bytes.
    let mut b_prev = [0u8; B_IN_BYTES];
    for (index, chunk) in out.chunks_mut(B_IN_BYTES).enumerate() {
        let mut hasher = Sha512::new();
        if index == 0 {
            hasher.update(b_0);
        } else {
            let mut xored = [0u8; B_IN_BYTES];
            for (x, (a, b)) in xored.iter_mut().zip(b_0.iter().zip(b_prev.iter())) {
                *x = a ^ b;
            }
            hasher.update(xored);
        }
        hasher.update([index as u8 + 1]);
        dst_prime(&mut hasher);
        b_prev = hasher.finalize().into();
        chunk.copy_from_slice(&b_prev[..chunk.len()]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curve25519_dalek::ristretto::CompressedRistretto;

    fn hex(s: &str) -> Vec<u8> { (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect() }

    /// RFC 9380 appendix K.3: expand_message_xmd with SHA-512. The expansion
    /// is checked at the 32- and 128-byte lengths the RFC publishes, which
    /// between them exercise the first block and the chained blocks that the
    /// 64-byte expansion of a leaf is made of.
    #[test]
    fn expand_message_xmd_sha512_matches_rfc_9380_vectors() {
        const DST: &[u8] = b"QUUX-V01-CS02-with-expander-SHA512-256";
        let q128 = format!("q128_{}", "q".repeat(128));
        let a512 = format!("a512_{}", "a".repeat(512));
        let vectors: [(&[u8], usize, &str); 10] = [
            (b"", 32, "6b9a7312411d92f921c6f68ca0b6380730a1a4d982c507211a90964c394179ba"),
            (b"abc", 32, "0da749f12fbe5483eb066a5f595055679b976e93abe9be6f0f6318bce7aca8dc"),
            (b"abcdef0123456789", 32, "087e45a86e2939ee8b91100af1583c4938e0f5fc6c9db4b107b83346bc967f58"),
            (q128.as_bytes(), 32, "7336234ee9983902440f6bc35b348352013becd88938d2afec44311caf8356b3"),
            (a512.as_bytes(), 32, "57b5f7e766d5be68a6bfe1768e3c2b7f1228b3e4b3134956dd73a59b954c66f4"),
            (
                b"",
                128,
                "41b037d1734a5f8df225dd8c7de38f851efdb45c372887be655212d07251b921b052b62eaed99b46f72f2ef4cc96bfaf254ebbbec091e1a3b9e4fb5e5b619d2e\
                 0c5414800a1d882b62bb5cd1778f098b8eb6cb399d5d9d18f5d5842cf5d13d7eb00a7cff859b605da678b318bd0e65ebff70bec88c753b159a805d2c89c55961",
            ),
            (
                b"abc",
                128,
                "7f1dddd13c08b543f2e2037b14cefb255b44c83cc397c1786d975653e36a6b11bdd7732d8b38adb4a0edc26a0cef4bb45217135456e58fbca1703cd6032cb134\
                 7ee720b87972d63fbf232587043ed2901bce7f22610c0419751c065922b488431851041310ad659e4b23520e1772ab29dcdeb2002222a363f0c2b1c972b3efe1",
            ),
            (
                b"abcdef0123456789",
                128,
                "3f721f208e6199fe903545abc26c837ce59ac6fa45733f1baaf0222f8b7acb0424814fcb5eecf6c1d38f06e9d0a6ccfbf85ae612ab8735dfdf9ce84c372a77c8\
                 f9e1c1e952c3a61b7567dd0693016af51d2745822663d0c2367e3f4f0bed827feecc2aaf98c949b5ed0d35c3f1023d64ad1407924288d366ea159f46287e61ac",
            ),
            (
                q128.as_bytes(),
                128,
                "b799b045a58c8d2b4334cf54b78260b45eec544f9f2fb5bd12fb603eaee70db7317bf807c406e26373922b7b8920fa29142703dd52bdf280084fb7ef69da78af\
                 df80b3586395b433dc66cde048a258e476a561e9deba7060af40adf30c64249ca7ddea79806ee5beb9a1422949471d267b21bc88e688e4014087a0b592b695ed",
            ),
            (
                a512.as_bytes(),
                128,
                "05b0bfef265dcee87654372777b7c44177e2ae4c13a27f103340d9cd11c86cb2426ffcad5bd964080c2aee97f03be1ca18e30a1f14e27bc11ebbd650f305269c\
                 c9fb1db08bf90bfc79b42a952b46daf810359e7bc36452684784a64952c343c52e5124cd1f71d474d5197fefc571a92929c9084ffe1112cf5eea5192ebff330b",
            ),
        ];
        for (msg, len, expected) in vectors {
            let mut out = vec![0u8; len];
            expand_message_xmd_sha512(msg, DST, &mut out);
            assert_eq!(out, hex(expected), "msg of {} bytes expanded to {len}", msg.len());
        }
    }

    /// RFC 9496 appendix A.3: element derivation from 64 uniform bytes, the
    /// map `hash_to_ristretto255` applies after the expansion. Checked here so
    /// the suite pins the curve library to the RFC's map.
    #[test]
    fn element_derivation_matches_rfc_9496_vectors() {
        let vectors = [
            (
                "5d1be09e3d0c82fc538112490e35701979d99e06ca3e2b5b54bffe8b4dc772c14d98b696a1bbfb5ca32c436cc61c16563790306c79eaca7705668b47dffe5bb6",
                "3066f82a1a747d45120d1740f14358531a8f04bbffe6a819f86dfe50f44a0a46",
            ),
            (
                "f116b34b8f17ceb56e8732a60d913dd10cce47a6d53bee9204be8b44f6678b270102a56902e2488c46120e9276cfe54638286b9e4b3cdb470b542d46c2068d38",
                "f26e5b6f7d362d2d2a94c5d0e7602cb4773c95a2e5c31a64f133189fa76ed61b",
            ),
        ];
        for (input, expected) in vectors {
            let uniform: [u8; 64] = hex(input).try_into().unwrap();
            let expected = CompressedRistretto(hex(expected).try_into().unwrap());
            assert_eq!(RistrettoPoint::from_uniform_bytes(&uniform).compress(), expected);
        }
    }
}
