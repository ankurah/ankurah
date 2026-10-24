use super::*;

fn bridge() -> DeltaContent { DeltaContent::EventBridge { events: vec![] } }

fn chunks(payload: Bytes, chunk_bytes: usize) -> Vec<EntityDeltaChunk> {
    EntityDeltaChunk::split(EntityId::from_bytes([1; 32]), payload, chunk_bytes).collect()
}

#[test]
fn split_slices_the_payload_in_order_without_copying() -> anyhow::Result<()> {
    let payload = bridge().encode_payload()?;
    let chunks = chunks(payload.clone(), 5);
    assert_eq!(chunks.iter().map(|chunk| chunk.offset).collect::<Vec<_>>(), [0, 5, 10]);
    assert_eq!(chunks.iter().map(|chunk| chunk.last).collect::<Vec<_>>(), [false, false, true]);
    let range = payload.as_ptr() as usize..payload.as_ptr() as usize + payload.len();
    assert!(chunks.iter().all(|chunk| chunk.bytes.len() <= 5 && range.contains(&(chunk.bytes.as_ptr() as usize))));
    assert_eq!(chunks.iter().flat_map(|chunk| chunk.bytes.to_vec()).collect::<Vec<_>>(), payload.to_vec());
    Ok(())
}

#[test]
fn the_assembler_decodes_only_after_the_last_chunk() -> anyhow::Result<()> {
    let payload = bridge().encode_payload()?;
    let mut assembler = DeltaAssembler::new(EntityId::from_bytes([1; 32]));
    let mut decoded = None;
    for chunk in chunks(payload.clone(), 5) {
        assert!(decoded.is_none(), "content arrives once, at the last chunk");
        decoded = assembler.push(chunk)?;
    }
    assert_eq!(decoded.map(|content| content.encode_payload()).transpose()?, Some(payload));
    Ok(())
}

#[test]
fn the_assembler_refuses_chunks_that_break_the_sequence() -> anyhow::Result<()> {
    let payload = bridge().encode_payload()?;
    let [first, second, _] = <[EntityDeltaChunk; 3]>::try_from(chunks(payload.clone(), 5)).unwrap();
    let assembler = || DeltaAssembler::new(EntityId::from_bytes([1; 32]));
    assert!(assembler().push(second.clone()).is_err(), "a delta starts at offset zero");
    assert!(assembler().push(EntityDeltaChunk { entity_id: EntityId::from_bytes([2; 32]), ..first.clone() }).is_err(), "another entity");
    assert!(assembler().push(EntityDeltaChunk { bytes: Bytes::new(), ..first.clone() }).is_err(), "an empty chunk");
    let mut repeated = assembler();
    repeated.push(first.clone())?;
    assert!(repeated.push(first).is_err(), "a chunk must continue where the last ended");
    let mut trailing = payload.to_vec();
    trailing.push(0);
    let mut assembler = assembler();
    let result = chunks(trailing.into(), 64).into_iter().try_fold(None, |_, chunk| assembler.push(chunk));
    assert!(result.is_err(), "bytes after the content");
    Ok(())
}
