//! One sample: bring a fresh fixture to the case's starting point, time the measured operation to
//! its endpoint, verify the result captured there, wait for the link to settle, and print the record.

use std::time::Instant;

use crate::{
    cpu::ProcessCpu,
    fixture::Fixture,
    installation::InstallationProbe,
    phase,
    record::{PhaseCpu, Record},
    scenario::Case,
    verify::InstalledResult,
};

/// A point in the measured interval: microseconds since the measured operation began, and the
/// process CPU reading there.
#[derive(Clone, Copy)]
struct Landmark {
    us: u128,
    cpu: Option<ProcessCpu>,
}

impl Landmark {
    /// The landmark at the present moment.
    fn reached(start: Instant) -> anyhow::Result<Self> { Ok(Self { us: start.elapsed().as_micros(), cpu: ProcessCpu::now()? }) }
}

/// Runs one sample of `case` on a fresh fixture and prints its record.
pub async fn run(case: &Case<'_>) -> anyhow::Result<()> {
    let selection = case.selection();
    let ordered = case.limit.is_some();
    let setup_cpu = ProcessCpu::now()?;
    let mut fixture = Fixture::new(case).await?;
    let cached_entities = fixture.prefetch(case, &selection).await?;
    let retained = case.mode.prepare(&mut fixture, case, &selection).await?;
    fixture.server.storage.take_reads();
    fixture.client.storage.take_reads();
    let probe = retained.as_ref().filter(|_| case.mode.reconnects()).map(|query| InstallationProbe::watch(query, &fixture.link, ordered));
    let (subscriptions_before, query_subscriptions_before) =
        fixture.link.subscriptions_handled(retained.as_ref().map(|query| query.query_id()));
    let required_query_subscription =
        retained.as_ref().map(|query| query.query_id()).zip(query_subscriptions_before).map(|(query, count)| (query, count + 1));
    fixture.link.start_measuring()?;

    phase("measure");
    let measurement_cpu = ProcessCpu::now()?;
    let start = Instant::now();
    let query = case.mode.measure(&fixture, &selection, retained).await?;
    let ready = if case.mode.reconnects() { None } else { Some(Landmark::reached(start)?) };
    let installation = match &probe {
        Some(probe) => {
            phase("wait-local-result-installed");
            Some(probe.wait(&query, &fixture.link).await?)
        }
        None => None,
    };
    let installed =
        installation.map(|installation| Landmark { us: installation.at.duration_since(start).as_micros(), cpu: installation.cpu });

    phase("verify-applied");
    let verification_cpu = ProcessCpu::now()?;
    let verification_start = Instant::now();
    // A reconnect captured its result inside the installation notification. Other scenarios capture
    // it here, right after the durable-answer wait, without waiting for the link to settle.
    let captured;
    let result = match installation {
        Some(installation) => &installation.result,
        None => {
            captured = InstalledResult::capture(&query.resultset(), ordered)?;
            &captured
        }
    };
    result.verify(&fixture.expected, &fixture.rows, &fixture.server, &fixture.client).await?;
    let verification_us = verification_start.elapsed().as_micros();
    let verified = Landmark::reached(start)?;
    let endpoint = ready.or(installed).expect("every scenario reaches the durable-answer or installation endpoint");

    // The activation's subscription response and a quiet link confirm the result already verified.
    let settled_at = fixture.link.settle(subscriptions_before + 1, required_query_subscription).await?;
    let settled = Landmark { us: settled_at.duration_since(start).as_micros(), cpu: ProcessCpu::now()? };
    // Work the nodes do after the link settles still counts; only the verifier's own point reads
    // of each expected entity are left out.
    let verified_entities = fixture.expected.members.len();
    let mut server_reads = fixture.server.storage.take_reads();
    let mut client_reads = fixture.client.storage.take_reads();
    server_reads.point_reads -= verified_entities;
    client_reads.point_reads -= verified_entities;
    let traffic = fixture.link.stop_measuring()?;

    let through = |landmark: Option<Landmark>| ProcessCpu::between(measurement_cpu, landmark.and_then(|landmark| landmark.cpu));
    Record {
        label: case.label,
        entities: case.entities,
        result_limit: case.limit,
        expected_results: fixture.expected.members.len(),
        selection: &selection,
        cache: case.cache,
        cached_entities,
        mode: case.mode,
        bandwidth_bytes_per_second: case.bytes_per_second,
        rtt_ms: case.rtt_ms,
        repeat: case.repeat,
        elapsed_us: endpoint.us,
        ready_us: ready.map(|ready| ready.us),
        local_result_installed_us: installed.map(|installed| installed.us),
        local_result_capture_us: installation.map(|installation| installation.capture_us),
        verified_us: verified.us,
        verification_us,
        transport_settled_us: settled.us,
        verified: true,
        traffic,
        server_reads,
        client_reads,
        phase_process_cpu: PhaseCpu {
            setup: ProcessCpu::between(setup_cpu, measurement_cpu),
            through_ready: through(ready),
            through_local_result_installed: through(installed),
            local_result_capture: installation.and_then(|installation| installation.capture_cpu),
            verification: ProcessCpu::between(verification_cpu, verified.cpu),
            through_verified: through(Some(verified)),
            through_transport_settled: through(Some(settled)),
            verified_to_transport_settled: ProcessCpu::between(verified.cpu, settled.cpu),
        },
    }
    .print()?;
    fixture.link.disconnect(&fixture.server, &fixture.client);
    Ok(())
}
