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
    use sha2::Sha256;

    /// The domain separation tag of RFC 9380 appendix K.3's vectors.
    const DST: &[u8] = b"QUUX-V01-CS02-with-expander-SHA512-256";

    fn hex(s: &str) -> Vec<u8> { (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect() }

    fn expand(msg: &[u8], dst: &[u8], len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        expand_message_xmd_sha512(msg, dst, &mut out);
        out
    }

    /// RFC 9380 appendix K.3: expand_message_xmd with SHA-512, at the 32- and
    /// 128-byte lengths the RFC publishes. These test the algorithm's
    /// branches, an output within the first block and blocks chained through
    /// strxor, but not the 64-byte answer a leaf uses: the requested length
    /// enters b_0, so no other length's answer contains it. The next test
    /// checks 64 bytes directly, and the leaf fixtures in leaf.rs
    /// (`leaf_points_match_known_answers`) are the production-length
    /// known-answer test, from a leaf's bytes to its point.
    #[test]
    fn expand_message_xmd_sha512_matches_rfc_9380_vectors() {
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

    /// The messages and tag of RFC 9380 appendix K.3, expanded to the 64 bytes
    /// a leaf uses. The answers come from a separate Python implementation of
    /// RFC 9380 section 5.3.1 on hashlib's SHA-512, which reproduces every
    /// appendix K.3 answer in the test above.
    #[test]
    fn expand_message_xmd_sha512_matches_independent_64_byte_answers() {
        let q128 = format!("q128_{}", "q".repeat(128));
        let a512 = format!("a512_{}", "a".repeat(512));
        let vectors: [(&[u8], &str); 5] = [
            (b"", "bb1edd5eb9d2013ba76c24410c8f54232fd258cdb088d54b1b3923f7deba035a10d9eee746edc2c6618ba48877d6a102ac850f9dde8d78d968abc9dc5658d851"),
            (b"abc", "4a05d1b49d7153fb512df83b8564fe1754c607e2fbbc3d97c591fa175b6fca1efb300462d96ed613f1534ecb260671eb8469a20071049dc8021b986828540592"),
            (
                b"abcdef0123456789",
                "ef58305dfa26469536b72eaa3dc6cb9f82a06b6d99c4a2bd60f24320b5e4a395b148ae89ce203dfda386f58a86f2533284356c8d437760b85ca5011deb2b9db3",
            ),
            (
                q128.as_bytes(),
                "8f091e488dabc12f3be9f70f5d14ec2a4d732b6d37f7813773fc6c91f8130ce7daac7283c059fe8e06dbeadcfe870bd2a5f40de96d702fb01ed53411600b9487",
            ),
            (
                a512.as_bytes(),
                "d3202e2019f687c6a9aff89e949d869d2e97544bf1404a02bea623fbb480606672481c4e42845b3b775155db6c650dcefaa829c88b38f4075be4af7cd6dc5bf6",
            ),
        ];
        for (msg, expected) in vectors {
            assert_eq!(expand(msg, DST, 64), hex(expected), "msg of {} bytes", msg.len());
        }
    }

    /// Lengths either side of one SHA-512 block, and the helper's limits, with
    /// answers from the same Python implementation: "abc" expanded to 63 and
    /// 65 bytes; to 16,320 bytes, the 255 blocks the expansion can produce,
    /// pinned by the SHA-256 of the output; and to 64 bytes under a 255-byte
    /// tag, the longest the helper takes, made of the bytes 0 to 254.
    #[test]
    fn expand_message_xmd_sha512_at_the_block_boundary_and_its_limits() {
        assert_eq!(
            expand(b"abc", DST, 63),
            hex("6efcc59228154a197acc37c8fd68b6d1e23abf1c952423a398e81083820d10db73c0b8976b0d4d6c13433c023d666b6b465050bf5395ec531c990d70cce8b2")
        );
        assert_eq!(
            expand(b"abc", DST, 65),
            hex("c7c619bcb1887e1384c7adec85a09b5818913db58b2fbb48db3cba2fe5397a27f73e3336e653ea2784a38a2d89e0fb2b1041e0e541d28907734aae67db42095a86")
        );
        let longest = expand(b"abc", DST, 255 * B_IN_BYTES);
        assert_eq!(Sha256::digest(&longest).to_vec(), hex("9278e866d9c3185b57dab028bdf9890f0d0756fd64c0228354838901947d1521"));
        let longest_tag: [u8; 255] = std::array::from_fn(|i| i as u8);
        assert_eq!(
            expand(b"abc", &longest_tag, 64),
            hex("7ed3b1694c070acf03a237397ba74e75a921a109e67f44999f58e31ef63431dfe4e44d36cb94dc9280e79c27081bbabdb685d79cab1449e3af4861b6c5b2f118")
        );
    }

    #[test]
    #[should_panic(expected = "expand_message_xmd produces at most 255 blocks")]
    fn expand_message_xmd_sha512_refuses_more_than_255_blocks() { expand(b"abc", DST, 255 * B_IN_BYTES + 1); }

    #[test]
    #[should_panic(expected = "domain separation tags are at most 255 bytes")]
    fn expand_message_xmd_sha512_refuses_a_tag_longer_than_255_bytes() { expand(b"abc", &[0u8; 256], 64); }

    /// RFC 9496 appendix A.3: element derivation from 64 uniform bytes, the
    /// map `hash_to_ristretto255` applies after the expansion. Checked here so
    /// the suite pins the curve library to the RFC's map; a separate Python
    /// implementation of RFC 9496 section 4.3.4 reproduces every answer.
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
            (
                "8422e1bbdaab52938b81fd602effb6f89110e1e57208ad12d9ad767e2e25510c27140775f9337088b982d83d7fcf0b2fa1edffe51952cbe7365e95c86eaf325c",
                "006ccd2a9e6867e6a2c5cea83d3302cc9de128dd2a9a57dd8ee7b9d7ffe02826",
            ),
            (
                "ac22415129b61427bf464e17baee8db65940c233b98afce8d17c57beeb7876c2150d15af1cb1fb824bbd14955f2b57d08d388aab431a391cfc33d5bafb5dbbaf",
                "f8f0c87cf237953c5890aec3998169005dae3eca1fbb04548c635953c817f92a",
            ),
            (
                "165d697a1ef3d5cf3c38565beefcf88c0f282b8e7dbd28544c483432f1cec7675debea8ebb4e5fe7d6f6e5db15f15587ac4d4d4a1de7191e0c1ca6664abcc413",
                "ae81e7dedf20a497e10c304a765c1767a42d6e06029758d2d7e8ef7cc4c41179",
            ),
            (
                "a836e6c9a9ca9f1e8d486273ad56a78c70cf18f0ce10abb1c7172ddd605d7fd2979854f47ae1ccf204a33102095b4200e5befc0465accc263175485f0e17ea5c",
                "e2705652ff9f5e44d3e841bf1c251cf7dddb77d140870d1ab2ed64f1a9ce8628",
            ),
            (
                "2cdc11eaeb95daf01189417cdddbf95952993aa9cb9c640eb5058d09702c74622c9965a697a3b345ec24ee56335b556e677b30e6f90ac77d781064f866a3c982",
                "80bd07262511cdde4863f8a7434cef696750681cb9510eea557088f76d9e5065",
            ),
        ];
        // Four inputs that all write the field elements 0 and 18: they differ
        // only in the top bit of each 32-byte half, which the derivation masks
        // off, and in halves written as p or more, which it reduces modulo p.
        // All four derive one element.
        let equivalent = "304282791023b73128d277bdcb5c7746ef2eac08dde9f2983379cb8e5ef0517f";
        let equivalent_inputs = [
            "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1200000000000000000000000000000000000000000000000000000000000000",
            "edffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
            "0000000000000000000000000000000000000000000000000000000000000080ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
            "00000000000000000000000000000000000000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000080",
        ];
        for (input, expected) in vectors.into_iter().chain(equivalent_inputs.map(|input| (input, equivalent))) {
            let uniform: [u8; 64] = hex(input).try_into().unwrap();
            let expected = CompressedRistretto(hex(expected).try_into().unwrap());
            assert_eq!(RistrettoPoint::from_uniform_bytes(&uniform).compress(), expected);
        }
    }
}
